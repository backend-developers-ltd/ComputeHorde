import logging
from collections import defaultdict

from asgiref.sync import sync_to_async
from compute_horde.subtensor import get_cycle_containing_block
from django.conf import settings

from compute_horde_validator.validator.models import Miner, OrganicJob, SyntheticJob
from compute_horde_validator.validator.scoring_new.calculations import (
    calculate_organic_scores,
    calculate_synthetic_scores,
    combine_scores,
)
from compute_horde_validator.validator.scoring_new.exceptions import (
    SplitDistributionError,
)
from compute_horde_validator.validator.scoring_new.interface import ScoringEngine
from compute_horde_validator.validator.scoring_new.models import (
    MinerSplit,
    MinerSplitDistribution,
    SplitInfo,
)
from compute_horde_validator.validator.scoring_new.split_querying import (
    query_miner_split_distributions,
)

logger = logging.getLogger(__name__)


class DefaultScoringEngine(ScoringEngine):
    """
    Default implementation of the scoring engine.
    """

    def __init__(self):
        self.dancing_bonus = getattr(settings, "DYNAMIC_DANCING_BONUS", 0.1)

    async def calculate_scores_for_cycles(
        self, current_cycle_start: int, previous_cycle_start: int, validator_hotkey: str
    ) -> dict[str, float]:
        """
        Calculate scores for two cycles and apply dancing.

        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            validator_hotkey: Validator hotkey for split retrieval

        Returns:
            Dictionary mapping hotkey to final score
        """
        logger.info(
            f"Calculating scores for cycles {current_cycle_start} and {previous_cycle_start}"
        )

        current_cycle_range = get_cycle_containing_block(
            block=current_cycle_start, netuid=settings.BITTENSOR_NETUID
        )

        current_organic_jobs: list[OrganicJob] = await sync_to_async(
            lambda: list(
                OrganicJob.objects.filter(
                    block__gte=current_cycle_start,
                    block__lt=current_cycle_range.stop,
                    cheated=False,
                    status=OrganicJob.Status.COMPLETED,
                    on_trusted_miner=False,
                ).select_related("miner")
            )
        )()

        current_synthetic_jobs: list[SyntheticJob] = await sync_to_async(
            lambda: list(
                SyntheticJob.objects.filter(
                    batch__cycle__start=current_cycle_start,
                    status=SyntheticJob.Status.COMPLETED,
                ).select_related("miner")
            )
        )()

        organic_scores = await calculate_organic_scores(current_organic_jobs)
        synthetic_scores = await calculate_synthetic_scores(current_synthetic_jobs)
        combined_scores = combine_scores(organic_scores, synthetic_scores)

        logger.info(f"Base scores calculated: {len(combined_scores)} hotkeys")

        final_scores = await self._apply_decoupled_dancing(
            combined_scores, current_cycle_start, previous_cycle_start, validator_hotkey
        )

        logger.info(f"Final scores calculated: {len(final_scores)} hotkeys")
        return final_scores

    async def _apply_decoupled_dancing(
        self,
        scores: dict[str, float],
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str,
    ) -> dict[str, float]:
        """
        Apply decoupled dancing to scores.

        Args:
            scores: Dictionary of hotkey -> score
            current_cycle_start: Current cycle start block
            previous_cycle_start: Previous cycle start block
            validator_hotkey: Validator hotkey

        Returns:
            Final scores with dancing applied
        """
        coldkey_scores, hotkey_to_coldkey = await self._group_scores_by_coldkey(scores)

        final_scores = await self._process_splits_and_distribute(
            coldkey_scores,
            hotkey_to_coldkey,
            scores,
            current_cycle_start,
            previous_cycle_start,
            validator_hotkey,
        )

        return final_scores

    async def _group_scores_by_coldkey(
        self, scores: dict[str, float]
    ) -> tuple[dict[str, float], dict[str, str]]:
        """
        Group scores by coldkey.

        Args:
            scores: Dictionary of hotkey -> score

        Returns:
            Tuple of (coldkey_scores, hotkey_to_coldkey_mapping)
        """
        coldkey_scores: defaultdict[str, float] = defaultdict(float)
        hotkey_to_coldkey: dict[str, str] = {}

        hotkeys_list = list(scores.keys())
        miners: list[Miner] = await sync_to_async(
            lambda: list(Miner.objects.filter(hotkey__in=hotkeys_list))
        )()
        miner_map = {miner.hotkey: miner for miner in miners}

        # Group by coldkey from database
        hotkeys_without_coldkey = []
        for hotkey, score in scores.items():
            miner = miner_map.get(hotkey)
            if miner and miner.coldkey:
                coldkey = miner.coldkey
                coldkey_scores[coldkey] += score
                hotkey_to_coldkey[hotkey] = coldkey
            else:
                hotkeys_without_coldkey.append(hotkey)
                if not miner:
                    logger.warning(f"Miner not found for hotkey: {hotkey}")

        # Handle hotkeys without coldkeys - keep as individual hotkeys
        for hotkey in hotkeys_without_coldkey:
            coldkey_scores[hotkey] = scores[hotkey]
            hotkey_to_coldkey[hotkey] = hotkey

        return dict(coldkey_scores), hotkey_to_coldkey

    async def _process_splits_and_distribute(
        self,
        coldkey_scores: dict[str, float],
        hotkey_to_coldkey: dict[str, str],
        original_scores: dict[str, float],
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str,
    ) -> dict[str, float]:
        """
        Process splits and distribute scores to hotkeys.

        Args:
            coldkey_scores: Scores grouped by coldkey
            hotkey_to_coldkey: Mapping from hotkey to coldkey
            original_scores: Original hotkey -> score mapping
            current_cycle_start: Current cycle start block
            previous_cycle_start: Previous cycle start block
            validator_hotkey: Validator hotkey

        Returns:
            Final scores distributed to hotkeys
        """
        final_scores = {}

        # Get coldkeys that need split processing
        coldkeys_for_splits = [
            coldkey for coldkey in coldkey_scores.keys() if coldkey not in original_scores
        ]

        current_splits = await self._query_current_splits(
            coldkeys_for_splits, current_cycle_start, validator_hotkey
        )

        # Get previous splits
        previous_splits = await self._get_split_distributions(
            coldkeys_for_splits, previous_cycle_start, validator_hotkey
        )

        # Process each coldkey
        for coldkey, total_score in coldkey_scores.items():
            # Handle individual hotkeys (no split processing needed)
            if coldkey in original_scores:
                final_scores[coldkey] = total_score
                continue

            distributed_scores = self._process_coldkey_split(
                coldkey,
                total_score,
                hotkey_to_coldkey,
                current_splits.get(coldkey),
                previous_splits.get(coldkey),
            )

            # Add distributed scores to final result
            for hotkey, score in distributed_scores.items():
                final_scores[hotkey] = score

        return final_scores

    async def _query_current_splits(
        self, coldkeys: list[str], cycle_start: int, validator_hotkey: str
    ) -> dict[str, SplitInfo]:
        """
        Query miners for current split distributions and save to database.

        Args:
            coldkeys: List of miner coldkeys
            cycle_start: Current cycle start block
            validator_hotkey: Validator hotkey

        Returns:
            Dictionary mapping coldkey to SplitInfo
        """
        if not coldkeys:
            return {}

        miners: list[Miner] = await sync_to_async(
            lambda: list(Miner.objects.filter(coldkey__in=coldkeys).select_related())
        )()

        split_distributions = await query_miner_split_distributions(miners)

        result = {}
        for coldkey in coldkeys:
            # Find first miner for this coldkey to get the split distribution
            miner_for_coldkey = None
            for miner in miners:
                if miner.coldkey == coldkey:
                    miner_for_coldkey = miner
                    break

            if miner_for_coldkey and miner_for_coldkey.hotkey in split_distributions:
                distributions_result = split_distributions[miner_for_coldkey.hotkey]

                if isinstance(distributions_result, Exception) or not distributions_result:
                    logger.warning(
                        f"Failed to get split distribution for {coldkey}: {distributions_result}"
                    )
                    continue

                distributions = distributions_result

                # Save to database
                await self._save_split(coldkey, cycle_start, validator_hotkey, distributions)

                result[coldkey] = SplitInfo(
                    coldkey=coldkey,
                    cycle_start=cycle_start,
                    validator_hotkey=validator_hotkey,
                    distributions=distributions,
                )

        return result

    async def _save_split(
        self, coldkey: str, cycle_start: int, validator_hotkey: str, distributions: dict[str, float]
    ):
        """
        Save split distribution to database.

        Args:
            coldkey: Miner coldkey
            cycle_start: Cycle start block
            validator_hotkey: Validator hotkey
            distributions: Distribution percentages by hotkey
        """
        try:
            split = await sync_to_async(MinerSplit.objects.create)(
                coldkey=coldkey,
                cycle_start=cycle_start,
                validator_hotkey=validator_hotkey,
            )

            for hotkey, percentage in distributions.items():
                await sync_to_async(MinerSplitDistribution.objects.create)(
                    split=split,
                    hotkey=hotkey,
                    percentage=percentage,
                )

            logger.debug(f"Saved split distribution for {coldkey}: {distributions}")

        except Exception as e:
            logger.error(f"Failed to save split distribution for {coldkey}: {e}")
            raise SplitDistributionError(f"Failed to save split distribution for {coldkey}: {e}")

    def _process_coldkey_split(
        self,
        coldkey: str,
        total_score: float,
        hotkey_to_coldkey: dict[str, str],
        current_split: SplitInfo | None,
        previous_split: SplitInfo | None,
    ) -> dict[str, float]:
        """
        Process split for a single coldkey and return distributed scores.

        Args:
            coldkey: Coldkey to process
            total_score: Total score for this coldkey
            hotkey_to_coldkey: Mapping from hotkey to coldkey
            current_split: Current split information
            previous_split: Previous split information

        Returns:
            Distributed scores by hotkey
        """
        if current_split:
            # Check for split changes to determine if dancing bonus should be applied
            dancing_bonus_applied = (
                previous_split and current_split.distributions != previous_split.distributions
            )

            if dancing_bonus_applied:
                logger.info(
                    f"Split distribution changed for coldkey: {coldkey}, applying dancing bonus"
                )
                # Apply dancing bonus logic: give bonus to main hotkey, distribute rest equally
                distributed_scores = self._apply_dancing_split_distribution(
                    total_score, current_split.distributions
                )
            else:
                # Apply normal split distribution
                distributed_scores = self._apply_split_distribution(
                    total_score, current_split.distributions
                )

            return distributed_scores
        else:
            # No split, distribute evenly among hotkeys in this coldkey
            hotkeys_in_coldkey = [
                hotkey for hotkey, ck in hotkey_to_coldkey.items() if ck == coldkey
            ]

            if hotkeys_in_coldkey:
                score_per_hotkey = total_score / len(hotkeys_in_coldkey)
                return {hotkey: score_per_hotkey for hotkey in hotkeys_in_coldkey}
            else:
                return {}

    async def _get_split_distributions(
        self, coldkeys: list[str], cycle_start: int, validator_hotkey: str
    ) -> dict[str, SplitInfo]:
        """
        Get split distributions for multiple coldkeys from database in a single query.

        Args:
            coldkeys: List of miner coldkeys
            cycle_start: Cycle start block
            validator_hotkey: Validator hotkey

        Returns:
            Dictionary mapping coldkey to SplitInfo
        """
        if not coldkeys:
            return {}
        splits: list[MinerSplit] = await sync_to_async(
            lambda: list(
                MinerSplit.objects.filter(
                    coldkey__in=coldkeys, cycle_start=cycle_start, validator_hotkey=validator_hotkey
                ).prefetch_related("distributions")
            )
        )()

        result = {}
        for split in splits:
            distributions = {}
            for distribution in split.distributions.all():
                distributions[distribution.hotkey] = float(distribution.percentage)

            result[split.coldkey] = SplitInfo(
                coldkey=split.coldkey,
                cycle_start=cycle_start,
                validator_hotkey=validator_hotkey,
                distributions=distributions,
            )

        return result

    def _apply_split_distribution(
        self, total_score: float, distributions: dict[str, float]
    ) -> dict[str, float]:
        """
        Apply split distribution to total score.

        Args:
            total_score: Total score for the coldkey
            distributions: Distribution percentages by hotkey

        Returns:
            Distributed scores by hotkey
        """
        distributed_scores = {}
        for hotkey, percentage in distributions.items():
            distributed_scores[hotkey] = total_score * percentage

        return distributed_scores

    def _apply_dancing_split_distribution(
        self, total_score: float, distributions: dict[str, float]
    ) -> dict[str, float]:
        """
        Apply dancing bonus to scores.

        Args:
            total_score: Total score for the coldkey
            distributions: Distribution percentages by hotkey

        Returns:
            Distributed scores by hotkey with bonus applied to main hotkey
        """
        # Find the hotkey with the highest percentage (main hotkey)
        main_hotkey = max(distributions.keys(), key=lambda k: distributions[k])
        main_percentage = distributions[main_hotkey]

        # Calculate the main hotkey's base share
        main_base_share = total_score * main_percentage

        # Apply bonus to the main hotkey's share
        bonus_multiplier = 1.0 + self.dancing_bonus
        main_bonus_share = main_base_share * bonus_multiplier

        # Calculate remaining score to distribute among other hotkeys
        remaining_score = total_score - main_base_share
        other_hotkeys = [hotkey for hotkey in distributions.keys() if hotkey != main_hotkey]

        # Distribute remaining score equally among other hotkeys
        if other_hotkeys:
            score_per_other_hotkey = remaining_score / len(other_hotkeys)
            distributed_scores = {hotkey: score_per_other_hotkey for hotkey in other_hotkeys}
        else:
            distributed_scores = {}

        # Add the bonus for the main hotkey
        distributed_scores[main_hotkey] = main_bonus_share

        return distributed_scores
