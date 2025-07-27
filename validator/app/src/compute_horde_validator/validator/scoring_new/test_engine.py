from datetime import timedelta
from unittest.mock import patch

import pytest
from asgiref.sync import async_to_sync
from compute_horde.protocol_messages import (
    V0MinerSplitDistributionRequest,
)
from django.conf import settings
from django.test import TestCase
from django.utils import timezone

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    OrganicJob,
    SyntheticJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.scoring_new.engine import DefaultScoringEngine
from compute_horde_validator.validator.scoring_new.models import (
    MinerSplit,
    MinerSplitDistribution,
)
from compute_horde_validator.validator.scoring_new.split_querying import (
    _query_single_miner_split_distribution,
)
from compute_horde_validator.validator.tests.transport import SimulationTransport


@pytest.mark.django_db(transaction=True)
class TestScoringEngine(TestCase):
    def setUp(self):
        super().setUp()

        self.miner1 = Miner.objects.create(
            coldkey="coldkey1", hotkey="hotkey1", address="127.0.0.1", port=8081
        )
        self.miner2 = Miner.objects.create(
            coldkey="coldkey2", hotkey="hotkey3", address="127.0.0.1", port=8082
        )
        self.miner3 = Miner.objects.create(
            hotkey="hotkey6", address="127.0.0.1", port=8083
        )

        self.current_cycle = Cycle.objects.create(start=1000, stop=1722)
        self.previous_cycle = Cycle.objects.create(start=278, stop=1000)

        self.current_batch = SyntheticJobBatch.objects.create(
            accepting_results_until=timezone.now() + timedelta(hours=1),
            block=1000,
            cycle=self.current_cycle,
        )
        self.previous_batch = SyntheticJobBatch.objects.create(
            accepting_results_until=timezone.now() + timedelta(hours=1),
            block=278,
            cycle=self.previous_cycle,
        )

        wallet = (
            settings.BITTENSOR_WALLET()
        )
        keypair = wallet.get_hotkey()
        self.validator_hotkey = keypair.ss58_address
        self.validator_keypair = keypair

        # Create simulation transports
        self.transport1 = SimulationTransport("miner1")
        self.transport2 = SimulationTransport("miner2")
        self.transport3 = SimulationTransport("miner3")

        async_to_sync(self.transport1.add_message)(
            V0MinerSplitDistributionRequest(
                split_distribution={"hotkey1": 0.8, "hotkey2": 0.2}
            ).model_dump_json(),
            send_before=2,
        )

        async_to_sync(self.transport2.add_message)(
            V0MinerSplitDistributionRequest(
                split_distribution={"hotkey3": 0.6, "hotkey4": 0.3, "hotkey5": 0.1}
            ).model_dump_json(),
            send_before=2,
        )

        async_to_sync(self.transport3.add_message)(
            V0MinerSplitDistributionRequest(split_distribution={"hotkey6": 1.0}).model_dump_json(),
            send_before=2,
        )

        self.transport_mapping = {
            "hotkey1": self.transport1,
            "hotkey3": self.transport2,
            "hotkey6": self.transport3,
        }

    def _get_mock_wstransport_constructor(self):
        """Get a mock WSTransport constructor that returns simulation transports."""
        def mock_wstransport_constructor(name, url, max_retries=2):
            miner_hotkey = name.replace("split_query_", "")
            if miner_hotkey in self.transport_mapping:
                return self.transport_mapping[miner_hotkey]
            else:
                from compute_horde.transport import WSTransport
                return WSTransport(name, url, max_retries)
        return mock_wstransport_constructor

    def _get_patch_target(self):
        """Get the correct patch target for WSTransport."""
        return 'compute_horde_validator.validator.scoring_new.split_querying.WSTransport'

    def test_calculate_scores_for_cycles_basic(self):
        """Test basic scoring."""
        
        with patch(self._get_patch_target(), side_effect=self._get_mock_wstransport_constructor()):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            OrganicJob.objects.create(
                miner=self.miner2,
                block=1001,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            SyntheticJob.objects.create(
                miner=self.miner1,
                batch=self.current_batch,
                status=SyntheticJob.Status.COMPLETED,
                score=10.0,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            SyntheticJob.objects.create(
                miner=self.miner3,
                batch=self.current_batch,
                status=SyntheticJob.Status.COMPLETED,
                score=15.0,
                miner_address_ip_version=4,
                miner_port=8083,
            )

            scores = async_to_sync(engine.calculate_scores_for_cycles)(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            # The original expected values were correct
            assert scores["hotkey1"] == 71.28
            assert scores["hotkey2"] == 17.82
            assert scores["hotkey3"] == 29.7
            assert scores["hotkey4"] == 14.85

    def test_calculate_scores_for_cycles_no_jobs(self):
        """Test scoring when there are no jobs."""
        
        with patch(self._get_patch_target(), side_effect=self._get_mock_wstransport_constructor()):
            engine = DefaultScoringEngine()

            scores = async_to_sync(engine.calculate_scores_for_cycles)(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores == {}

    def test_calculate_scores_for_cycles_with_dancing_bonus(self):
        """Test scoring with dancing bonus when splits change."""
        
        with patch(self._get_patch_target(), side_effect=self._get_mock_wstransport_constructor()):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            previous_split = MinerSplit.objects.create(
                coldkey="coldkey1", cycle_start=278, validator_hotkey=self.validator_hotkey
            )
            MinerSplitDistribution.objects.create(
                split=previous_split,
                hotkey="hotkey1",
                percentage=0.5,
            )

            scores = async_to_sync(engine.calculate_scores_for_cycles)(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == 87.12
            assert scores["hotkey2"] == pytest.approx(19.8)

    def test_calculate_scores_for_cycles_with_cheated_organic_jobs(self):
        """Test scoring with cheated organic jobs - they should be excluded."""
        
        with patch(self._get_patch_target(), side_effect=self._get_mock_wstransport_constructor()):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=True,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            scores = async_to_sync(engine.calculate_scores_for_cycles)(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == 79.2
            assert scores["hotkey2"] == 19.8

    def test_calculate_scores_for_cycles_with_failed_organic_jobs(self):
        """Test scoring with failed organic jobs - they should be excluded."""
        
        with patch(self._get_patch_target(), side_effect=self._get_mock_wstransport_constructor()):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.FAILED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            SyntheticJob.objects.create(
                miner=self.miner1,
                batch=self.current_batch,
                status=SyntheticJob.Status.COMPLETED,
                score=10.0,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            scores = async_to_sync(engine.calculate_scores_for_cycles)(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == 79.2
            assert scores["hotkey2"] == 19.8

    def test_calculate_scores_for_cycles_with_excused_synthetic_jobs(self):
        """Test scoring with excused synthetic jobs."""
        
        with patch(self._get_patch_target(), side_effect=self._get_mock_wstransport_constructor()):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            SyntheticJob.objects.create(
                miner=self.miner1,
                batch=self.current_batch,
                status=SyntheticJob.Status.EXCUSED,
                score=15,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            scores = async_to_sync(engine.calculate_scores_for_cycles)(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == 79.2
            assert scores["hotkey2"] == 19.8

    def test_calculate_scores_for_cycles_with_trusted_miner_jobs(self):
        """Test scoring with trusted miner jobs."""
        
        with patch(self._get_patch_target(), side_effect=self._get_mock_wstransport_constructor()):
            engine = DefaultScoringEngine()

            # Create trusted miner organic job - should be excluded from scoring
            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                on_trusted_miner=True,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            # Create normal organic job - should be included
            OrganicJob.objects.create(
                miner=self.miner2,
                block=1001,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                on_trusted_miner=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            SyntheticJob.objects.create(
                miner=self.miner1,
                batch=self.current_batch,
                status=SyntheticJob.Status.COMPLETED,
                score=10.0,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            scores = async_to_sync(engine.calculate_scores_for_cycles)(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == 79.2
            assert scores["hotkey2"] == 19.8
