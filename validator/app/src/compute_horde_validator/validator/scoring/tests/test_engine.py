import asyncio
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
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
from compute_horde_validator.validator.scoring.engine import DefaultScoringEngine
from compute_horde_validator.validator.scoring.models import (
    MinerSplit,
    MinerSplitDistribution,
)
from compute_horde_validator.validator.tests.transport import SimulationTransport


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
            coldkey="coldkey3", hotkey="hotkey6", address="127.0.0.1", port=8083
        )

        # Add miners for the other hotkeys that appear in split distributions
        self.miner_hotkey2 = Miner.objects.create(
            coldkey="coldkey1", hotkey="hotkey2", address="127.0.0.1", port=8084
        )
        self.miner_hotkey4 = Miner.objects.create(
            coldkey="coldkey2", hotkey="hotkey4", address="127.0.0.1", port=8085
        )
        self.miner_hotkey5 = Miner.objects.create(
            coldkey="coldkey2", hotkey="hotkey5", address="127.0.0.1", port=8086
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

        wallet = settings.BITTENSOR_WALLET()
        keypair = wallet.get_hotkey()
        self.validator_hotkey = keypair.ss58_address
        self.validator_keypair = keypair

        # Create simulation transports
        self.transport1 = SimulationTransport("miner1")
        self.transport2 = SimulationTransport("miner2")
        self.transport3 = SimulationTransport("miner3")

        asyncio.run(self._setup_transports())

    async def _setup_transports(self):
        """Set up transport messages asynchronously."""
        self._reset_transports()

        await self.transport1.add_message(
            V0MinerSplitDistributionRequest(
                split_distribution={"hotkey1": 0.8, "hotkey2": 0.2}
            ).model_dump_json(),
            send_before=2,
        )

        await self.transport2.add_message(
            V0MinerSplitDistributionRequest(
                split_distribution={"hotkey3": 0.6, "hotkey4": 0.3, "hotkey5": 0.1}
            ).model_dump_json(),
            send_before=2,
        )

        await self.transport3.add_message(
            V0MinerSplitDistributionRequest(split_distribution={"hotkey6": 1.0}).model_dump_json(),
            send_before=2,
        )

        self.transport_mapping = {
            "hotkey1": self.transport1,
            "hotkey3": self.transport2,
            "hotkey6": self.transport3,
        }

    def tearDown(self):
        """Clean up after each test to ensure isolation."""
        super().tearDown()
        # Reset transport state
        self._reset_transports()

    def _reset_transports(self):
        """Reset transport state to ensure test isolation."""
        for transport in [self.transport1, self.transport2, self.transport3]:
            transport.received.clear()
            transport.sent.clear()
            transport.receive_at_counter = 0
            transport.to_receive.clear()

    def _get_mock_wstransport_constructor(self):
        """Get a mock WSTransport constructor that returns simulation transports."""

        def mock_wstransport_constructor(name, url, max_retries=2):
            miner_hotkey = name.replace("split_query_", "")
            if miner_hotkey in self.transport_mapping:
                return self.transport_mapping[miner_hotkey]
            else:
                from compute_horde.transport import WSTransport

                return WSTransport(name, url, max_retries=max_retries)

        return mock_wstransport_constructor

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_basic(self):
        """Test basic scoring.

        Score calculation:
        - 1 organic job for miner1 (score=1.0)
        - 1 organic job for miner2 (score=1.0)
        - 1 synthetic job for miner1 (score=10.0)
        - 1 synthetic job for miner3 (score=15.0)
        - Raw scores: hotkey1=1.0, hotkey3=1.0, hotkey6=15.0
        - Split distribution: hotkey1=80%, hotkey2=20% (for coldkey1), hotkey3=60%, hotkey4=30%, hotkey5=10% (for coldkey2), hotkey6=100% (for coldkey3)
        - Normalization per executor class (total across all coldkeys):
        - Total executor class score: 1.0 + 1.0 + 10.0 + 15.0 = 27.0
        - hotkey1: (8.8 / 27.0) * 99 = 32.27
        - hotkey2: (2.2 / 27.0) * 99 = 8.07
        - hotkey3: (0.6 / 27.0) * 99 = 2.20
        - hotkey4: (0.3 / 27.0) * 99 = 1.10
        - hotkey5: (0.1 / 27.0) * 99 = 0.37
        - hotkey6: (15.0 / 27.0) * 99 = 55.0
        """

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
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

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == pytest.approx(32.27, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(8.07, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(2.20, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(1.10, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(0.37, abs=0.01)
            assert scores["hotkey6"] == pytest.approx(55.0, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_no_jobs(self):
        """Test scoring when there are no jobs."""

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
            engine = DefaultScoringEngine()

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores == {}

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_dancing_bonus(self):
        """Test scoring with dancing bonus when splits change.

        Score calculation:
        - Raw score: hotkey1=1.0 (1 organic job)
        - Previous split: hotkey1=50%, hotkey2=50%
        - Current split: hotkey1=80%, hotkey2=20% (different distribution)
        - Dancing bonus applied: +10% to hotkey1 (80.67 vs 79.2 without bonus)
        """

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
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

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == pytest.approx(80.67, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(18.33, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_cheated_organic_jobs(self):
        """Test scoring with cheated organic jobs - they should be excluded.

        Score calculation:
        - 1 cheated organic job for miner 1 (excluded from scoring)
        - 1 valid organic job for miner1 (score=1.0)
        - 1 valid organic job for miner2 (score=1.0)
        - Raw scores: hotkey1=1.0, hotkey3=1.0
        - Split distribution: hotkey1=80%, hotkey2=20% (for coldkey1), hotkey3=60%, hotkey4=30%, hotkey5=10% (for coldkey2)
        - Normalization per executor class (total across all coldkeys):
        - Total executor class score: 0.8 + 0.2 + 0.6 + 0.3 + 0.1 = 2.0
        - hotkey1: (0.8 / 2.0) * 99 = 39.6
        - hotkey2: (0.2 / 2.0) * 99 = 9.9
        - hotkey3: (0.6 / 2.0) * 99 = 29.7
        - hotkey4: (0.3 / 2.0) * 99 = 14.85
        - hotkey5: (0.1 / 2.0) * 99 = 4.95
        """

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
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
            OrganicJob.objects.create(
                miner=self.miner2,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == pytest.approx(39.6, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(9.9, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(29.7, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(14.85, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(4.95, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_failed_organic_jobs(self):
        """Test scoring with failed organic jobs - they should be excluded.

        Score calculation:
        - 1 failed organic job for miner 1 (excluded from scoring)
        - 1 synthetic job for miner1 (score=10.0)
        - 1 organic job for miner2 (score=1.0)
        - Raw scores: hotkey1=10.0, hotkey3=1.0
        - Split distribution: hotkey1=80%, hotkey2=20% (for coldkey1), hotkey3=60%, hotkey4=30%, hotkey5=10% (for coldkey2)
        - Normalization per executor class (total across all coldkeys):
        - Total executor class score: 8.0 + 2.0 + 0.6 + 0.3 + 0.1 = 11.0
        - hotkey1: (8.0 / 11.0) * 99 = 72.0
        - hotkey2: (2.0 / 11.0) * 99 = 18.0
        - hotkey3: (0.6 / 11.0) * 99 = 5.4
        - hotkey4: (0.3 / 11.0) * 99 = 2.7
        - hotkey5: (0.1 / 11.0) * 99 = 0.9
        """

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
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

            OrganicJob.objects.create(
                miner=self.miner2,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == pytest.approx(72.0, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(18.0, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(5.4, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(2.7, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(0.9, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_excused_synthetic_jobs(self):
        """Test scoring with excused synthetic jobs.

        Score calculation:
        - 2 organic jobs for miner1 (score=2.0)
        - 1 excused synthetic job (excluded from scoring)
        - 1 organic job for miner2 (score=1.0)
        - Raw scores: hotkey1=2.0, hotkey3=1.0
        - Split distribution: hotkey1=80%, hotkey2=20% (for coldkey1), hotkey3=60%, hotkey4=30%, hotkey5=10% (for coldkey2)
        - Normalization per executor class (total across all coldkeys):
        - Total executor class score: 1.6 + 0.4 + 0.6 + 0.3 + 0.1 = 3.0
        - hotkey1: (1.6 / 3.0) * 99 = 52.8
        - hotkey2: (0.4 / 3.0) * 99 = 13.2
        - hotkey3: (0.6 / 3.0) * 99 = 19.8
        - hotkey4: (0.3 / 3.0) * 99 = 9.9
        - hotkey5: (0.1 / 3.0) * 99 = 3.3
        """

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
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

            OrganicJob.objects.create(
                miner=self.miner2,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == pytest.approx(52.8, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(13.2, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(19.8, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(9.9, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(3.3, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_trusted_miner_jobs(self):
        """Test scoring with trusted miner jobs.

        Score calculation:
        - 1 trusted miner organic job for miner 1 (excluded from scoring)
        - 1 regular organic job for miner1 (score=1.0)
        - 1 synthetic job for miner2 (score=10.0)
        - Raw scores: hotkey1=1.0, hotkey3=10.0
        - Split distribution: hotkey1=80%, hotkey2=20% (for coldkey1), hotkey3=60%, hotkey4=30%, hotkey5=10% (for coldkey2)
        - Normalization per executor class (total across all coldkeys):
        - Total executor class score: 0.8 + 0.2 + 6.0 + 3.0 + 1.0 = 11.0
        - hotkey1: (0.8 / 11.0) * 99 = 7.2
        - hotkey2: (0.2 / 11.0) * 99 = 1.8
        - hotkey3: (6.0 / 11.0) * 99 = 54.0
        - hotkey4: (3.0 / 11.0) * 99 = 27.0
        - hotkey5: (1.0 / 11.0) * 99 = 9.0
        """

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                on_trusted_miner=True,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            OrganicJob.objects.create(
                miner=self.miner1,
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                on_trusted_miner=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            SyntheticJob.objects.create(
                miner=self.miner2,
                batch=self.current_batch,
                status=SyntheticJob.Status.COMPLETED,
                score=10.0,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == pytest.approx(7.2, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(1.8, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(54.0, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(27.0, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(9.0, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_different_executor_classes(self):
        """Test scoring with different executor classes to verify weight application.

        Score calculation:
        - 5 organic jobs in default executor class (spin_up-4min.gpu-24gb, weight=99)
        - 11 organic jobs in LLM executor class (always_on.llm.a6000, weight=1)
        - All jobs for hotkey1, so raw scores: default=5.0, LLM=11.0
        - Split distribution: hotkey1=80%, hotkey2=20%
        """

        with patch(
            "compute_horde_validator.validator.scoring.split_querying.WSTransport",
            side_effect=self._get_mock_wstransport_constructor(),
        ):
            engine = DefaultScoringEngine()
            for _ in range(5):
                OrganicJob.objects.create(
                    miner=self.miner1,
                    block=1000,
                    status=OrganicJob.Status.COMPLETED,
                    cheated=False,
                    executor_class="spin_up-4min.gpu-24gb",
                    miner_address_ip_version=4,
                    miner_port=8081,
                )

            for _ in range(11):
                OrganicJob.objects.create(
                    miner=self.miner2,
                    block=1000,
                    status=OrganicJob.Status.COMPLETED,
                    cheated=False,
                    executor_class="always_on.llm.a6000",
                    miner_address_ip_version=4,
                    miner_port=8081,
                )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey=self.validator_hotkey,
            )

            assert scores["hotkey1"] == pytest.approx(79.2, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(19.8, abs=0.01)

    @pytest.mark.django_db
    def test_query_current_splits_multiple_hotkeys_for_coldkey(self):
        """Test that multiple hotkeys are tried for the same coldkey when first fails."""

        miner1 = MagicMock(spec=Miner)
        miner1.coldkey = "coldkey1"
        miner1.hotkey = "hotkey1"

        miner2 = MagicMock(spec=Miner)
        miner2.coldkey = "coldkey1"  # Same coldkey
        miner2.hotkey = "hotkey2"

        miner3 = MagicMock(spec=Miner)
        miner3.coldkey = "coldkey2"
        miner3.hotkey = "hotkey3"

        mock_split_distributions = {
            "hotkey1": Exception("Connection failed"),  # First hotkey fails
            "hotkey2": {"hotkey_a": 0.6, "hotkey_b": 0.4},  # Second hotkey succeeds
            "hotkey3": {"hotkey_c": 1.0},  # Different coldkey
        }

        with patch(
            "compute_horde_validator.validator.scoring.engine.query_miner_split_distributions",
            return_value=mock_split_distributions,
        ):
            with patch(
                "compute_horde_validator.validator.models.Miner.objects.filter"
            ) as mock_filter:
                mock_filter.return_value.select_related.return_value = [miner1, miner2, miner3]

                engine = DefaultScoringEngine()
                result = engine._query_current_splits(
                    coldkeys=["coldkey1", "coldkey2"],
                    cycle_start=1000,
                    validator_hotkey="validator1",
                )

                assert "coldkey1" in result
                assert result["coldkey1"].distributions == {"hotkey_a": 0.6, "hotkey_b": 0.4}

                assert "coldkey2" in result
                assert result["coldkey2"].distributions == {"hotkey_c": 1.0}

    @pytest.mark.django_db
    def test_query_current_splits_all_hotkeys_fail(self):
        """Test that when all hotkeys for a coldkey fail, it's skipped."""

        miner1 = MagicMock(spec=Miner)
        miner1.coldkey = "coldkey1"
        miner1.hotkey = "hotkey1"

        miner2 = MagicMock(spec=Miner)
        miner2.coldkey = "coldkey1"
        miner2.hotkey = "hotkey2"

        mock_split_distributions = {
            "hotkey1": Exception("Connection failed"),
            "hotkey2": Exception("Connection failed"),
        }

        with patch(
            "compute_horde_validator.validator.scoring.engine.query_miner_split_distributions",
            return_value=mock_split_distributions,
        ):
            with patch(
                "compute_horde_validator.validator.models.Miner.objects.filter"
            ) as mock_filter:
                mock_filter.return_value.select_related.return_value = [miner1, miner2]

                engine = DefaultScoringEngine()
                result = engine._query_current_splits(
                    coldkeys=["coldkey1"], cycle_start=1000, validator_hotkey="validator1"
                )

                assert result == {}
