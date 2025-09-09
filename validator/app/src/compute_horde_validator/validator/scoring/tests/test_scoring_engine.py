import asyncio
import uuid
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
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
    MinerMainHotkey,
)
from compute_horde_validator.validator.tests.transport import SimulationTransport


class TestMainHotkeyScoringEngine(TestCase):
    def setUp(self):
        super().setUp()

        self.miner1 = Miner.objects.create(
            coldkey="coldkey1", hotkey="hotkey1", address="127.0.0.1", port=8081
        )
        self.miner2 = Miner.objects.create(
            coldkey="coldkey2", hotkey="hotkey3", address="127.0.0.1", port=8082
        )
        self.miner3 = Miner.objects.create(
            coldkey="coldkey3", hotkey="hotkey7", address="127.0.0.1", port=8083
        )

        self.miner_hotkey2 = Miner.objects.create(
            coldkey="coldkey1", hotkey="hotkey2", address="127.0.0.1", port=8084
        )
        self.miner_hotkey4 = Miner.objects.create(
            coldkey="coldkey2", hotkey="hotkey4", address="127.0.0.1", port=8085
        )
        self.miner_hotkey5 = Miner.objects.create(
            coldkey="coldkey2", hotkey="hotkey5", address="127.0.0.1", port=8086
        )
        self.miner_hotkey7 = Miner.objects.create(
            coldkey="coldkey2", hotkey="hotkey6", address="127.0.0.1", port=8087
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

        self.transport_mapping = {
            "hotkey1": self.transport1,
            "hotkey2": self.transport1,
            "hotkey3": self.transport2,
            "hotkey4": self.transport2,
            "hotkey5": self.transport2,
            # No response from hotkey 6
            "hotkey7": self.transport3,
        }

    def tearDown(self):
        super().tearDown()
        self._reset_transports()

    def _reset_transports(self):
        for transport in [self.transport1, self.transport2, self.transport3]:
            transport.received.clear()
            transport.sent.clear()
            transport.receive_at_counter = 0
            transport.to_receive.clear()

    def _get_mock_wstransport_constructor(self):
        """Get a mock WSTransport constructor that returns simulation transports."""

        def mock_wstransport_constructor(name, url, max_retries=2):
            if ":8081" in url:
                return self.transport1
            elif ":8082" in url:
                return self.transport2
            elif ":8083" in url:
                return self.transport3
            elif ":8084" in url:
                return self.transport1
            elif ":8085" in url:
                return self.transport2
            elif ":8086" in url:
                return self.transport2
            else:
                raise ValueError(f"Unknown URL: {url}")

        return mock_wstransport_constructor

    def _get_mock_http_responses(self):
        """Get a mock HTTP response for _query_single_miner."""

        async def mock_http_response(miner):
            # Return the main hotkey based on the miner's hotkey
            # This matches the original test setup where each miner returns its own hotkey as main hotkey
            return miner.hotkey

        return mock_http_response

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_basic(self):
        """Test basic scoring with main hotkey logic.

        Score calculation:
        - 1 organic job for miner1 (score=1.0)
        - 1 organic job for miner2 (score=1.0)
        - 1 synthetic job for miner1 (score=10.0)
        - 1 synthetic job for miner3 (score=15.0)
        - Raw scores: coldkey=1.0, coldkey2=1.0, coldkey3=15.0
        - Normalization per executor class (total across all coldkeys):
        - Total executor class score: 1.0 + 1.0 + 10.0 + 15.0 = 27.0
        - hotkey1: (8.8 / 27.0) * 99 = 32.27
        - hotkey2: (2.2 / 27.0) * 99 = 8.07
        - hotkey3: (0.8 / 27.0) * 99 = 2.93
        - hotkey4: (0.065 / 27.0) * 99 = 0.24
        - hotkey5: (0.065 / 27.0) * 99 = 0.24
        - hotkey6: (0.065 / 27.0) * 99 = 0.24
        - hotkey7: (15.0 / 27.0) * 99 = 55.0
        """

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            OrganicJob.objects.create(
                miner=self.miner2,
                job_uuid=uuid.uuid4(),
                block=1000,
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
            )

            assert scores["hotkey1"] == pytest.approx(32.27, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(8.07, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(2.93, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(0.24, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(0.24, abs=0.01)
            assert scores["hotkey6"] == pytest.approx(0.24, abs=0.01)
            assert scores["hotkey7"] == pytest.approx(55.0, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_no_jobs(self):
        """Test scoring when there are no jobs."""

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
            )

            assert scores == {}

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_dancing_bonus(self):
        """Test scoring with dancing bonus when main hotkey changes.

        Score calculation:
        - Raw score: hotkey1=1.0 (1 organic job)
        - Previous main hotkey: hotkey2 (different from current)
        - Current main hotkey: hotkey1 (different distribution)
        - Dancing bonus applied: +10% to total score (1.0 * 1.1 = 1.1)
        - Main hotkey distribution: hotkey1=80% (0.88), hotkey2=20% (0.22)
        - Normalization: total = 1.1, hotkey1 = (0.88 / 1.1) * 99 = 79.2, hotkey2 = (0.22 / 1.1) * 99 = 19.8
        """

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            # Create previous main hotkey record with different main hotkey
            MinerMainHotkey.objects.create(
                coldkey="coldkey1",
                cycle_start=278,
                main_hotkey="hotkey2",  # Different from current main_hotkey="hotkey1"
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
            )

            assert scores["hotkey1"] == pytest.approx(79.2, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(19.8, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_cheated_organic_jobs(self):
        """Test scoring with cheated organic jobs - they should be excluded.

        Score calculation:
        - 1 cheated organic job for miner 1 (excluded from scoring)
        - 1 valid organic job for miner1 (score=1.0)
        - 1 valid organic job for miner2 (score=1.0)
        - Raw scores: coldkey1=1.0, coldkey2=1.0
        - Total executor class score: 0.8 + 0.2 + 0.6 + 0.3 + 0.1 = 2.0
        - hotkey1: (0.8 / 2.0) * 99 = 39.6
        - hotkey2: (0.2 / 2.0) * 99 = 9.9
        - hotkey3: (0.8 / 2.0) * 99 = 39.6
        - hotkey4: (0.66 / 2.0) * 99 = 3.3
        - hotkey5: (0.66 / 2.0) * 99 = 3.3
        - hotkey6: (0.66 / 2.0) * 99 = 3.3
        """

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=True,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            OrganicJob.objects.create(
                miner=self.miner2,
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
            )

            assert scores["hotkey1"] == pytest.approx(39.6, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(9.9, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(39.6, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(3.3, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(3.3, abs=0.01)
            assert scores["hotkey6"] == pytest.approx(3.3, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_failed_organic_jobs(self):
        """Test scoring with failed organic jobs - they should be excluded.

        Score calculation:
        - 1 failed organic job for miner 1 (excluded from scoring)
        - 1 synthetic job for miner1 (score=10.0)
        - 1 organic job for miner2 (score=1.0)
        - Raw scores: coldkey1=10.0, coldkey2=1.0
        - Total executor class score: 8.0 + 2.0 + 0.6 + 0.3 + 0.1 = 11.0
        - hotkey1: (8.0 / 11.0) * 99 = 72.0
        - hotkey2: (2.0 / 11.0) * 99 = 18.0
        - hotkey3: (0.8 / 11.0) * 99 = 7.2
        - hotkey4: (0.066 / 11.0) * 99 = 0.6
        - hotkey5: (0.066 / 11.0) * 99 = 0.6
        - hotkey6: (0.066 / 11.0) * 99 = 0.6
        """

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
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
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
            )

            assert scores["hotkey1"] == pytest.approx(72.0, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(18.0, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(7.2, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(0.6, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(0.6, abs=0.01)
            assert scores["hotkey6"] == pytest.approx(0.6, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_excused_synthetic_jobs(self):
        """Test scoring with excused synthetic jobs.

        Score calculation:
        - 2 organic jobs for miner1 (score=2.0)
        - 1 excused synthetic job (excluded from scoring)
        - 1 organic job for miner2 (score=1.0)
        - Raw scores: coldkey1=2.0, coldkey2=1.0
        - Total executor class score: 1.6 + 0.4 + 0.6 + 0.3 + 0.1 = 3.0
        - hotkey1: (1.6 / 3.0) * 99 = 52.8
        - hotkey2: (0.4 / 3.0) * 99 = 13.2
        - hotkey3: (0.6 / 3.0) * 99 = 26.4
        - hotkey4: (0.066 / 3.0) * 99 = 2.2
        - hotkey5: (0.066 / 3.0) * 99 = 2.2
        - hotkey6: (0.066 / 3.0) * 99 = 2.2
        """

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8081,
            )

            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
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
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                miner_address_ip_version=4,
                miner_port=8082,
            )

            scores = engine.calculate_scores_for_cycles(
                current_cycle_start=1000,
                previous_cycle_start=278,
            )

            assert scores["hotkey1"] == pytest.approx(52.8, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(13.2, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(26.4, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(2.2, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(2.2, abs=0.01)
            assert scores["hotkey6"] == pytest.approx(2.2, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_trusted_miner_jobs(self):
        """Test scoring with trusted miner jobs.

        Score calculation:
        - 1 trusted miner organic job for miner 1 (excluded from scoring)
        - 1 regular organic job for miner1 (score=1.0)
        - 1 synthetic job for miner2 (score=10.0)
        - Raw scores: coldkey1=1.0, coldkey2=10.0
        - Total executor class score: 0.8 + 0.2 + 6.0 + 3.0 + 1.0 = 11.0
        - hotkey1: (0.8 / 11.0) * 99 = 7.2
        - hotkey2: (0.2 / 11.0) * 99 = 1.8
        - hotkey3: (6.0 / 11.0) * 99 = 72.0
        - hotkey4: (0.66 / 11.0) * 99 = 6.0
        - hotkey5: (0.66 / 11.0) * 99 = 6.0
        - hotkey6: (0.66 / 11.0) * 99 = 6.0
        """

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()

            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
                block=1000,
                status=OrganicJob.Status.COMPLETED,
                cheated=False,
                on_trusted_miner=True,
                miner_address_ip_version=4,
                miner_port=8081,
            )
            OrganicJob.objects.create(
                miner=self.miner1,
                job_uuid=uuid.uuid4(),
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
            )

            assert scores["hotkey1"] == pytest.approx(7.2, abs=0.01)
            assert scores["hotkey2"] == pytest.approx(1.8, abs=0.01)
            assert scores["hotkey3"] == pytest.approx(72.0, abs=0.01)
            assert scores["hotkey4"] == pytest.approx(6.0, abs=0.01)
            assert scores["hotkey5"] == pytest.approx(6.0, abs=0.01)
            assert scores["hotkey6"] == pytest.approx(6.0, abs=0.01)

    @pytest.mark.django_db(transaction=True)
    def test_calculate_scores_for_cycles_with_different_executor_classes(self):
        """Test scoring with different executor classes to verify weight application.

        Score calculation:
        - 5 organic jobs in default executor class (spin_up-4min.gpu-24gb, weight=99)
        - 11 organic jobs in LLM executor class (always_on.llm.a6000, weight=1)
        - All jobs for hotkey1, so raw scores: default=5.0, LLM=11.0
        """

        with patch(
            "compute_horde_validator.validator.scoring.main_hotkey_querying._query_single_miner",
            side_effect=self._get_mock_http_responses(),
        ):
            engine = DefaultScoringEngine()
            for _ in range(5):
                OrganicJob.objects.create(
                    miner=self.miner1,
                    job_uuid=uuid.uuid4(),
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
                    job_uuid=uuid.uuid4(),
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

        mock_main_hotkeys = {
            "hotkey1": Exception("Connection failed"),  # First hotkey fails
            "hotkey2": "hotkey1",  # Second hotkey succeeds
            "hotkey3": "hotkey3",  # Different coldkey
        }

        with patch(
            "compute_horde_validator.validator.scoring.engine.query_miner_main_hotkeys",
            return_value=mock_main_hotkeys,
        ):
            with patch(
                "compute_horde_validator.validator.models.Miner.objects.filter"
            ) as mock_filter:
                mock_filter.return_value.select_related.return_value = [miner1, miner2, miner3]

                engine = DefaultScoringEngine()
                result = engine._query_current_main_hotkeys(
                    coldkeys=["coldkey1", "coldkey2"],
                    cycle_start=1000,
                )

                assert "coldkey1" in result
                assert result["coldkey1"].main_hotkey == "hotkey1"

                assert "coldkey2" in result
                assert result["coldkey2"].main_hotkey == "hotkey3"

    @pytest.mark.django_db
    def test_query_current_splits_all_hotkeys_fail(self):
        """Test that when all hotkeys for a coldkey fail, it's skipped."""

        miner1 = MagicMock(spec=Miner)
        miner1.coldkey = "coldkey1"
        miner1.hotkey = "hotkey1"

        miner2 = MagicMock(spec=Miner)
        miner2.coldkey = "coldkey1"
        miner2.hotkey = "hotkey2"

        mock_main_hotkeys = {
            "hotkey1": Exception("Connection failed"),
            "hotkey2": Exception("Connection failed"),
        }

        with patch(
            "compute_horde_validator.validator.scoring.engine.query_miner_main_hotkeys",
            return_value=mock_main_hotkeys,
        ):
            with patch(
                "compute_horde_validator.validator.models.Miner.objects.filter"
            ) as mock_filter:
                mock_filter.return_value.select_related.return_value = [miner1, miner2]

                engine = DefaultScoringEngine()
                result = engine._query_current_main_hotkeys(coldkeys=["coldkey1"], cycle_start=1000)

                assert result == {}

    @pytest.mark.django_db
    def test_query_current_main_hotkeys_uses_existing_database_data(self):
        """Test that when main hotkeys already exist in database, they are returned without external queries."""

        # Create existing main hotkey records in the database
        MinerMainHotkey.objects.create(coldkey="coldkey1", cycle_start=1000, main_hotkey="hotkey1")
        MinerMainHotkey.objects.create(coldkey="coldkey2", cycle_start=1000, main_hotkey="hotkey3")

        # Mock the external query function - it should not be called
        with patch(
            "compute_horde_validator.validator.scoring.engine.query_miner_main_hotkeys"
        ) as mock_query:
            mock_query.return_value = {}

            engine = DefaultScoringEngine()
            result = engine._query_current_main_hotkeys(
                coldkeys=["coldkey1", "coldkey2"],
                cycle_start=1000,
            )

            mock_query.assert_not_called()

            assert "coldkey1" in result
            assert result["coldkey1"].main_hotkey == "hotkey1"
            assert result["coldkey1"].cycle_start == 1000

            assert "coldkey2" in result
            assert result["coldkey2"].main_hotkey == "hotkey3"
            assert result["coldkey2"].cycle_start == 1000
