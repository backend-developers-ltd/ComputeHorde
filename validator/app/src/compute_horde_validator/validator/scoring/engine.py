"""
Main scoring engine for calculating weights with decoupled dancing.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, Optional

from django.conf import settings
from django.db.models import Q

from ..models import Miner, OrganicJob, SyntheticJob
from .calculations import calculate_organic_scores, calculate_synthetic_scores, combine_scores
from .models import SplitInfo, SplitStorage, MinerSplit, MinerSplitDistribution

logger = logging.getLogger(__name__)


class ScoringEngine(ABC):
    """
    Abstract interface for scoring engines that calculate scores for cycles and apply decoupled dancing.
    """
    
    @abstractmethod
    async def calculate_scores_for_cycles(
        self, 
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str
    ) -> Dict[str, float]:
        """
        Calculate scores for two cycles and apply decoupled dancing.
        
        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            validator_hotkey: Validator hotkey for split retrieval
            
        Returns:
            Dictionary mapping hotkey to final score
        """
        pass


class DefaultScoringEngine(ScoringEngine):
    """
    Default implementation of the scoring engine that calculates scores for cycles and applies decoupled dancing.
    """
    
    def __init__(self):
        self.split_storage = SplitStorage()
        self.dancing_bonus = getattr(settings, 'DYNAMIC_DANCING_BONUS', 0.1)
    
    async def calculate_scores_for_cycles(
        self, 
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str
    ) -> Dict[str, float]:
        """
        Calculate scores for two cycles and apply decoupled dancing.
        
        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            validator_hotkey: Validator hotkey for split retrieval
            
        Returns:
            Dictionary mapping hotkey to final score
        """
        logger.info(f"Calculating scores for cycles {current_cycle_start} and {previous_cycle_start}")
        
        # Get jobs from current cycle
        current_organic_jobs = await OrganicJob.objects.filter(
            block__gte=current_cycle_start,
            block__lt=current_cycle_start + 722
        ).all()
        
        current_synthetic_jobs = await SyntheticJob.objects.filter(
            batch__cycle__start=current_cycle_start
        ).all()
        
        # Calculate scores (without dancing)
        organic_scores = calculate_organic_scores(current_organic_jobs)
        synthetic_scores = calculate_synthetic_scores(current_synthetic_jobs)
        combined_scores = combine_scores(organic_scores, synthetic_scores)
        
        logger.info(f"Base scores calculated: {len(combined_scores)} hotkeys")
        
        # Apply decoupled dancing
        final_scores = await self._apply_decoupled_dancing(
            combined_scores,
            current_cycle_start,
            previous_cycle_start,
            validator_hotkey
        )
        
        logger.info(f"Final scores calculated: {len(final_scores)} hotkeys")
        return final_scores
    
    async def _apply_decoupled_dancing(
        self,
        scores: Dict[str, float],
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str
    ) -> Dict[str, float]:
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
        # Group scores by coldkey
        coldkey_scores = defaultdict(float)
        hotkey_to_coldkey = {}
        
        print(f"DEBUG: Processing scores: {scores}")
        
        # First, try to get coldkeys from database
        hotkeys_without_coldkey = []
        for hotkey, score in scores.items():
            try:
                miner = await Miner.objects.aget(hotkey=hotkey)
                coldkey = miner.coldkey
                print(f"DEBUG: Found miner for {hotkey}: coldkey={coldkey}")
                if coldkey:
                    coldkey_scores[coldkey] += score
                    hotkey_to_coldkey[hotkey] = coldkey
                else:
                    hotkeys_without_coldkey.append(hotkey)
            except Miner.DoesNotExist:
                hotkeys_without_coldkey.append(hotkey)
                logger.warning(f"Miner not found for hotkey: {hotkey}")
        
        # If we have hotkeys without coldkeys, try to get them from Bittensor
        if hotkeys_without_coldkey:
            from .calculations import get_hotkey_to_coldkey_mapping
            bittensor_mapping = get_hotkey_to_coldkey_mapping(hotkeys_without_coldkey)
            
            for hotkey in hotkeys_without_coldkey:
                if hotkey in bittensor_mapping:
                    coldkey = bittensor_mapping[hotkey]
                    print(f"DEBUG: Got coldkey from Bittensor for {hotkey}: coldkey={coldkey}")
                    coldkey_scores[coldkey] += scores[hotkey]
                    hotkey_to_coldkey[hotkey] = coldkey
                else:
                    print(f"DEBUG: No coldkey found for {hotkey}, keeping as individual hotkey")
                    # Keep as individual hotkey (no coldkey grouping)
                    coldkey_scores[hotkey] = scores[hotkey]
                    hotkey_to_coldkey[hotkey] = hotkey
        
        print(f"DEBUG: coldkey_scores = {dict(coldkey_scores)}")
        print(f"DEBUG: hotkey_to_coldkey = {hotkey_to_coldkey}")
        
        # Get splits for each coldkey
        final_scores = {}
        
        for coldkey, total_score in coldkey_scores.items():
            print(f"DEBUG: Processing coldkey {coldkey} with total_score {total_score}")
            
            # Skip split logic for individual hotkeys (coldkey == hotkey)
            if coldkey in scores:
                print(f"DEBUG: {coldkey} is an individual hotkey, no split applied")
                final_scores[coldkey] = total_score
                continue
            
            # Get current split distribution
            current_split = await self._get_split_distribution(
                coldkey, current_cycle_start, validator_hotkey
            )
            
            print(f"DEBUG: current_split for {coldkey} = {current_split}")
            
            if current_split:
                print(f"DEBUG: Found split for {coldkey}, distributions = {current_split.distributions}")
                # Apply split distribution
                distributed_scores = self._apply_split_distribution(
                    total_score, current_split.distributions
                )
                print(f"DEBUG: distributed_scores = {distributed_scores}")
                
                # Check for split changes and apply bonus
                if await self._has_split_change(
                    coldkey, current_cycle_start, previous_cycle_start, validator_hotkey
                ):
                    distributed_scores = self._apply_dancing_bonus(distributed_scores)
                    logger.info(f"Applied dancing bonus to coldkey: {coldkey}")
                
                for hotkey, value in distributed_scores.items():
                    final_scores[hotkey] = final_scores.get(hotkey, 0) + value
            else:
                print(f"DEBUG: No split found for {coldkey}, distributing evenly")
                # No split, distribute evenly among hotkeys in this coldkey
                hotkeys_in_coldkey = [
                    hotkey for hotkey, ck in hotkey_to_coldkey.items() 
                    if ck == coldkey
                ]
                if hotkeys_in_coldkey:
                    score_per_hotkey = total_score / len(hotkeys_in_coldkey)
                    for hotkey in hotkeys_in_coldkey:
                        final_scores[hotkey] = score_per_hotkey
        
        print(f"DEBUG: Final scores = {final_scores}")
        return final_scores
    
    async def _get_split_distribution(
        self, 
        coldkey: str, 
        cycle_start: int, 
        validator_hotkey: str
    ) -> Optional[SplitInfo]:
        """
        Get split distribution for a coldkey from database.
        
        Args:
            coldkey: Miner coldkey
            cycle_start: Cycle start block
            validator_hotkey: Validator hotkey
            
        Returns:
            SplitInfo if found, None otherwise
        """
        try:
            split = await MinerSplit.objects.aget(
                coldkey=coldkey,
                cycle_start=cycle_start,
                validator_hotkey=validator_hotkey
            )
            
            distributions = {}
            async for distribution in split.distributions.all():
                distributions[distribution.hotkey] = float(distribution.percentage)
            
            return SplitInfo(
                coldkey=coldkey,
                cycle_start=cycle_start,
                validator_hotkey=validator_hotkey,
                distributions=distributions
            )
        except MinerSplit.DoesNotExist:
            return None
    
    def _apply_split_distribution(
        self, 
        total_score: float, 
        distributions: Dict[str, float]
    ) -> Dict[str, float]:
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
    
    async def _has_split_change(
        self,
        coldkey: str,
        current_cycle: int,
        previous_cycle: int,
        validator_hotkey: str
    ) -> bool:
        """
        Check if there was a split change between cycles.
        
        Args:
            coldkey: Miner coldkey
            current_cycle: Current cycle start
            previous_cycle: Previous cycle start
            validator_hotkey: Validator hotkey
            
        Returns:
            True if split changed, False otherwise
        """
        current_split = await self._get_split_distribution(
            coldkey, current_cycle, validator_hotkey
        )
        previous_split = await self._get_split_distribution(
            coldkey, previous_cycle, validator_hotkey
        )
        
        if current_split is None or previous_split is None:
            return False
        
        return current_split.distributions != previous_split.distributions
    
    def _apply_dancing_bonus(self, scores: Dict[str, float]) -> Dict[str, float]:
        """
        Apply dancing bonus to scores.
        
        Args:
            scores: Scores by hotkey
            
        Returns:
            Scores with bonus applied
        """
        bonus_multiplier = 1.0 + self.dancing_bonus
        return {hotkey: score * bonus_multiplier for hotkey, score in scores.items()} 