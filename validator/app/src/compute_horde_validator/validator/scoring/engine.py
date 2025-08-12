import logging
from collections import defaultdict

from compute_horde.subtensor import get_cycle_containing_block
from django.conf import settings

from compute_horde_validator.validator.dynamic_config import get_executor_class_weights
from compute_horde_validator.validator.models import Miner, OrganicJob, SyntheticJob
from compute_horde_validator.validator.scoring.calculations import (
    calculate_organic_scores,
    calculate_synthetic_scores,
    combine_scores,
)
from compute_horde_validator.validator.scoring.exceptions import (
    MainHotkeyError,
)
from compute_horde_validator.validator.scoring.interface import ScoringEngine
from compute_horde_validator.validator.scoring.main_hotkey_querying import (
    query_miner_main_hotkeys,
)
from compute_horde_validator.validator.scoring.models import (
    MainHotkeyInfo,
    MinerMainHotkey,
)

logger = logging.getLogger(__name__)


class DefaultScoringEngine(ScoringEngine):
    def __init__(self):
        self.dancing_bonus = getattr(settings, "DYNAMIC_DANCING_BONUS", 0.1)
        self.main_hotkey_share = getattr(settings, "MAIN_HOTKEY_SHARE", 0.8)

    def calculate_scores_for_cycles(
        self, current_cycle_start: int, previous_cycle_start: int
    ) -> dict[str, float]:
        """
        Calculate scores for current cycle.

        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            validator_hotkey: Validator hotkey for split retrieval

        Returns:
            Dictionary mapping hotkey to final score
        """
        logger.info(f"Calculating scores for cycle {current_cycle_start}")

        hotkey_to_coldkey = self._create_hotkey_to_coldkey_mapping()

        current_cycle_range = get_cycle_containing_block(
            block=current_cycle_start, netuid=settings.BITTENSOR_NETUID
        )

        current_organic_jobs: list[OrganicJob] = list(
            OrganicJob.objects.filter(
                block__gte=current_cycle_start,
                block__lt=current_cycle_range.stop,
                cheated=False,
                status=OrganicJob.Status.COMPLETED,
                on_trusted_miner=False,
            ).select_related("miner")
        )

        current_synthetic_jobs: list[SyntheticJob] = list(
            SyntheticJob.objects.filter(
                batch__cycle__start=current_cycle_start,
                status=SyntheticJob.Status.COMPLETED,
            ).select_related("miner")
        )

        organic_scores = calculate_organic_scores(current_organic_jobs)
        synthetic_scores = calculate_synthetic_scores(current_synthetic_jobs)

        combined_scores_by_executor = combine_scores(organic_scores, synthetic_scores)
        logger.info(f"Base scores calculated for cycle {current_cycle_start}")

        executor_class_weights = get_executor_class_weights()
        logger.info(f"Executor class weights calculated for cycle {current_cycle_start}")

        normalized_scores: dict[str, float] = {}

        for executor_class, executor_scores in combined_scores_by_executor.items():
            weight = executor_class_weights.get(executor_class, 1.0)

            final_executor_scores = self._apply_dancing(
                executor_scores,
                current_cycle_start,
                previous_cycle_start,
                hotkey_to_coldkey,
            )

            total_executor_score = sum(final_executor_scores.values())
            if total_executor_score > 0:
                normalized_executor_scores = {
                    hotkey: (score / total_executor_score) * weight
                    for hotkey, score in final_executor_scores.items()
                }
                # Accumulate scores from different executor classes
                for hotkey, score in normalized_executor_scores.items():
                    normalized_scores[hotkey] = normalized_scores.get(hotkey, 0) + score

        logger.info(f"Final scores calculated for cycle {current_cycle_start}")
        return normalized_scores

    def _create_hotkey_to_coldkey_mapping(self) -> dict[str, str]:
        """
        Create mapping from hotkey to coldkey for all miners.

        Returns:
            Dictionary mapping hotkey to coldkey
        """
        hotkey_to_coldkey: dict[str, str] = {}

        all_miners: list[Miner] = list(Miner.objects.all())
        for miner in all_miners:
            if miner.coldkey:
                hotkey_to_coldkey[miner.hotkey] = miner.coldkey
            else:
                # Handle hotkeys without coldkeys - map to themselves
                hotkey_to_coldkey[miner.hotkey] = miner.hotkey

        return hotkey_to_coldkey

    def _apply_dancing(
        self,
        scores: dict[str, float],
        current_cycle_start: int,
        previous_cycle_start: int,
        hotkey_to_coldkey: dict[str, str],
    ) -> dict[str, float]:
        """
        Apply dancing to scores.

        Args:
            scores: Dictionary of hotkey -> score
            current_cycle_start: Current cycle start block
            previous_cycle_start: Previous cycle start block
            validator_hotkey: Validator hotkey
            hotkey_to_coldkey: Mapping from hotkey to coldkey

        Returns:
            Final scores with dancing applied
        """
        coldkey_scores = self._group_scores_by_coldkey(scores, hotkey_to_coldkey)

        final_scores = self._process_main_hotkeys_and_distribute(
            coldkey_scores,
            hotkey_to_coldkey,
            scores,
            current_cycle_start,
            previous_cycle_start,
        )

        return final_scores

    def _group_scores_by_coldkey(
        self, scores: dict[str, float], hotkey_to_coldkey: dict[str, str]
    ) -> dict[str, float]:
        """
        Group scores by coldkey.

        Args:
            scores: Dictionary of hotkey -> score
            hotkey_to_coldkey: Mapping from hotkey to coldkey

        Returns:
            Dictionary mapping coldkey to total score
        """
        coldkey_scores: defaultdict[str, float] = defaultdict(float)

        for hotkey, score in scores.items():
            coldkey = hotkey_to_coldkey.get(hotkey)
            if coldkey:
                coldkey_scores[coldkey] += score
            else:
                # Handle hotkeys not in mapping - keep as individual hotkeys
                coldkey_scores[hotkey] = score
                logger.warning(f"Hotkey {hotkey} not found in hotkey_to_coldkey mapping")

        return dict(coldkey_scores)

    def _process_main_hotkeys_and_distribute(
        self,
        coldkey_scores: dict[str, float],
        hotkey_to_coldkey: dict[str, str],
        original_scores: dict[str, float],
        current_cycle_start: int,
        previous_cycle_start: int,
    ) -> dict[str, float]:
        """
        Process main hotkeys and distribute scores to hotkeys.

        Args:
            coldkey_scores: Scores grouped by coldkey
            hotkey_to_coldkey: Mapping from hotkey to coldkey
            original_scores: Original hotkey -> score mapping
            current_cycle_start: Current cycle start block
            previous_cycle_start: Previous cycle start block

        Returns:
            Final scores distributed to hotkeys
        """
        final_scores = {}

        coldkeys_for_main_hotkeys = [
            coldkey for coldkey in coldkey_scores.keys() if coldkey not in original_scores
        ]

        current_main_hotkeys = self._query_current_main_hotkeys(
            coldkeys_for_main_hotkeys, current_cycle_start
        )

        previous_main_hotkeys = self._get_main_hotkey(
            coldkeys_for_main_hotkeys, previous_cycle_start
        )

        for coldkey, total_score in coldkey_scores.items():
            if coldkey in original_scores:
                final_scores[coldkey] = total_score
                continue

            distributed_scores = self._process_coldkey_main_hotkey(
                coldkey,
                total_score,
                hotkey_to_coldkey,
                current_main_hotkeys.get(coldkey),
                previous_main_hotkeys.get(coldkey),
            )

            for hotkey, score in distributed_scores.items():
                final_scores[hotkey] = score

        return final_scores

    def _query_current_main_hotkeys(
        self, coldkeys: list[str], cycle_start: int
    ) -> dict[str, MainHotkeyInfo]:
        """
        Query miners for current main hotkeys.

        Args:
            coldkeys: List of miner coldkeys
            cycle_start: Current cycle start block

        Returns:
            Dictionary mapping coldkey to MainHotkeyInfo
        """
        if not coldkeys:
            return {}

        miners: list[Miner] = list(Miner.objects.filter(coldkey__in=coldkeys).select_related())

        main_hotkeys = query_miner_main_hotkeys(miners)

        result = {}
        for coldkey in coldkeys:
            # Find all miners for this coldkey
            miners_for_coldkey = [miner for miner in miners if miner.coldkey == coldkey]

            if not miners_for_coldkey:
                logger.warning(f"No miners found for coldkey {coldkey}")
                continue

            main_hotkey = None
            successful_hotkey = None

            for miner in miners_for_coldkey:
                if miner.hotkey in main_hotkeys:
                    main_hotkey_result = main_hotkeys[miner.hotkey]

                    if isinstance(main_hotkey_result, Exception) or not main_hotkey_result:
                        logger.debug(
                            f"Failed to get main hotkey for {coldkey} using hotkey {miner.hotkey}: {main_hotkey_result}"
                        )
                        continue

                    main_hotkey = main_hotkey_result
                    successful_hotkey = miner.hotkey
                    logger.info(
                        f"Successfully got main hotkey for {coldkey} using hotkey {successful_hotkey}: {main_hotkey}"
                    )
                    break

            if main_hotkey is None:
                logger.warning(
                    f"Failed to get main hotkey for {coldkey} using any of its hotkeys: {[m.hotkey for m in miners_for_coldkey]}"
                )
                continue

            self._save_main_hotkey(coldkey, cycle_start, main_hotkey)

            result[coldkey] = MainHotkeyInfo(
                coldkey=coldkey,
                cycle_start=cycle_start,
                main_hotkey=main_hotkey,
            )

        return result

    def _save_main_hotkey(self, coldkey: str, cycle_start: int, main_hotkey: str | None):
        """
        Save main hotkey to database.

        Args:
            coldkey: Miner coldkey
            cycle_start: Cycle start block
            main_hotkey: Main hotkey for this coldkey
        """
        try:
            MinerMainHotkey.objects.create(
                coldkey=coldkey,
                cycle_start=cycle_start,
                main_hotkey=main_hotkey,
            )

            logger.debug(f"Saved main hotkey for {coldkey}: {main_hotkey}")

        except Exception as e:
            logger.error(f"Failed to save main hotkey for {coldkey}: {e}")
            raise MainHotkeyError(f"Failed to save main hotkey for {coldkey}: {e}")

    def _process_coldkey_main_hotkey(
        self,
        coldkey: str,
        total_score: float,
        hotkey_to_coldkey: dict[str, str],
        current_main_hotkey: MainHotkeyInfo | None,
        previous_main_hotkey: MainHotkeyInfo | None,
    ) -> dict[str, float]:
        """
        Process main hotkey for a single coldkey and return distributed scores.

        Args:
            coldkey: Coldkey to process
            total_score: Total score for this coldkey
            hotkey_to_coldkey: Mapping from hotkey to coldkey
            current_main_hotkey: Current main hotkey information
            previous_main_hotkey: Previous main hotkey information

        Returns:
            Distributed scores by hotkey
        """
        if current_main_hotkey and current_main_hotkey.main_hotkey:
            # Check for main hotkey changes to determine if dancing bonus should be applied
            dancing_bonus_applied = (
                previous_main_hotkey
                and previous_main_hotkey.main_hotkey
                and current_main_hotkey.main_hotkey != previous_main_hotkey.main_hotkey
            )

            if dancing_bonus_applied:
                logger.info(f"Main hotkey changed for coldkey: {coldkey}, applying dancing bonus")
                # Apply dancing bonus with main hotkey share
                distributed_scores = self._apply_dancing_main_hotkey_distribution(
                    total_score, current_main_hotkey.main_hotkey, hotkey_to_coldkey, coldkey
                )
            else:
                # Apply normal main hotkey distribution
                distributed_scores = self._apply_main_hotkey_distribution(
                    total_score, current_main_hotkey.main_hotkey, hotkey_to_coldkey, coldkey
                )

            return distributed_scores
        else:
            # No main hotkey, distribute evenly among all hotkeys in this coldkey
            hotkeys_in_coldkey = [
                hotkey for hotkey, ck in hotkey_to_coldkey.items() if ck == coldkey
            ]

            if hotkeys_in_coldkey:
                score_per_hotkey = total_score / len(hotkeys_in_coldkey)
                return {hotkey: score_per_hotkey for hotkey in hotkeys_in_coldkey}
            else:
                return {}

    def _get_main_hotkey(self, coldkeys: list[str], cycle_start: int) -> dict[str, MainHotkeyInfo]:
        """
        Get main hotkey for multiple coldkeys.

        Args:
            coldkeys: List of miner coldkeys
            cycle_start: Cycle start block

        Returns:
            Dictionary mapping coldkey to MainHotkeyInfo
        """
        if not coldkeys:
            return {}
        main_hotkeys: list[MinerMainHotkey] = list(
            MinerMainHotkey.objects.filter(coldkey__in=coldkeys, cycle_start=cycle_start)
        )

        result = {}
        for main_hotkey_record in main_hotkeys:
            result[main_hotkey_record.coldkey] = MainHotkeyInfo(
                coldkey=main_hotkey_record.coldkey,
                cycle_start=cycle_start,
                main_hotkey=main_hotkey_record.main_hotkey,
            )

        return result

    def _apply_main_hotkey_distribution(
        self, total_score: float, main_hotkey: str, hotkey_to_coldkey: dict[str, str], coldkey: str
    ) -> dict[str, float]:
        """
        Apply main hotkey distribution to total score.

        Args:
            total_score: Total score for the coldkey
            main_hotkey: Main hotkey for this coldkey
            hotkey_to_coldkey: Mapping from hotkey to coldkey
            coldkey: Coldkey being processed

        Returns:
            Distributed scores by hotkey
        """
        # Get all hotkeys for this coldkey
        hotkeys_in_coldkey = [hotkey for hotkey, ck in hotkey_to_coldkey.items() if ck == coldkey]

        if main_hotkey in hotkeys_in_coldkey:
            # If there's only one hotkey in this coldkey, give it 100% of the score
            if len(hotkeys_in_coldkey) == 1:
                distributed_scores = {main_hotkey: total_score}
            else:
                # Multiple hotkeys: apply main hotkey share distribution
                main_hotkey_score = total_score * self.main_hotkey_share
                remaining_score = total_score - main_hotkey_score
                other_hotkeys = [hotkey for hotkey in hotkeys_in_coldkey if hotkey != main_hotkey]

                distributed_scores = {main_hotkey: main_hotkey_score}

                # Distribute remaining score equally among other hotkeys
                score_per_other_hotkey = remaining_score / len(other_hotkeys)
                for hotkey in other_hotkeys:
                    distributed_scores[hotkey] = score_per_other_hotkey
        else:
            # Main hotkey is not registered, distribute evenly among all hotkeys
            score_per_hotkey = total_score / len(hotkeys_in_coldkey)
            distributed_scores = {hotkey: score_per_hotkey for hotkey in hotkeys_in_coldkey}

        return distributed_scores

    def _apply_dancing_main_hotkey_distribution(
        self, total_score: float, main_hotkey: str, hotkey_to_coldkey: dict[str, str], coldkey: str
    ) -> dict[str, float]:
        """
        Apply dancing bonus with main hotkey distribution.

        Args:
            total_score: Total score for the coldkey
            main_hotkey: Main hotkey for this coldkey
            hotkey_to_coldkey: Mapping from hotkey to coldkey
            coldkey: Coldkey being processed

        Returns:
            Distributed scores by hotkey with bonus applied
        """
        # Get all hotkeys for this coldkey
        hotkeys_in_coldkey = [hotkey for hotkey, ck in hotkey_to_coldkey.items() if ck == coldkey]

        bonus_multiplier = 1.0 + self.dancing_bonus
        total_score_with_bonus = total_score * bonus_multiplier

        if main_hotkey in hotkeys_in_coldkey:
            # If there's only one hotkey in this coldkey, give it 100% of the score with bonus
            if len(hotkeys_in_coldkey) == 1:
                distributed_scores = {main_hotkey: total_score_with_bonus}
            else:
                # Multiple hotkeys: apply main hotkey share distribution with bonus
                main_hotkey_score = total_score_with_bonus * self.main_hotkey_share
                remaining_score = total_score_with_bonus - main_hotkey_score
                other_hotkeys = [hotkey for hotkey in hotkeys_in_coldkey if hotkey != main_hotkey]

                distributed_scores = {main_hotkey: main_hotkey_score}

                # Distribute remaining score equally among other hotkeys
                score_per_other_hotkey = remaining_score / len(other_hotkeys)
                for hotkey in other_hotkeys:
                    distributed_scores[hotkey] = score_per_other_hotkey
        else:
            # Main hotkey is not registered, distribute evenly among all hotkeys
            score_per_hotkey = total_score_with_bonus / len(hotkeys_in_coldkey)
            distributed_scores = {hotkey: score_per_hotkey for hotkey in hotkeys_in_coldkey}

        return distributed_scores
