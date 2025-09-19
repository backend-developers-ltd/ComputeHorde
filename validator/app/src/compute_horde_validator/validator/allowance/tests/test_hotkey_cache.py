import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.tests.mockchain import set_block_number
from compute_horde_validator.validator.allowance.utils import blocks, supertensor


@pytest.mark.django_db(transaction=True)
def test_allowance_uses_current_validator_hotkey_not_stale():
    """RED TEST (expected to fail before fix):

    Demonstrates the root cause of flakiness: the Allowance singleton caches the validator
    hotkey at construction time. If the underlying supertensor instance (and thus wallet/hotkey)
    changes afterwards (as tests do via `set_block_number`), allowances are created for the *new*
    hotkey, but queries still filter by the *old* cached one, yielding a misleading
    NotEnoughAllowanceException with zeros.

    Expected (desired) behaviour: Even if the supertensor instance changes after the Allowance
    singleton is first created, `find_miners_with_allowance` should reflect the new validator
    hotkey and return miners.

    Current (buggy) behaviour: This test will fail because no miners are returned (exception).
    After the fix (making the validator hotkey dynamic), this test will pass.
    """

    # Stage 1: Instantiate allowance inside first mocked block context (captures first wallet hotkey)
    with set_block_number(1000):
        st = supertensor.supertensor()  # capture existing instance before fixture blocks new init
        old_hotkey = allowance().my_ss58_address
        blocks.process_block_allowance_with_reporting(1000, supertensor_=st)

    # Stage 2: Patch the existing supertensor's wallet hotkey AFTER Allowance cached old_hotkey
    class DummyHotkey:
        def __init__(self, ss58_address):
            self.ss58_address = ss58_address

    class DummyWallet:
        def __init__(self, hotkey):
            self._hotkey = hotkey

        def get_hotkey(self):
            return self._hotkey

    new_hotkey_value = old_hotkey + "_DIFF"
    dummy_wallet = DummyWallet(DummyHotkey(new_hotkey_value))
    original_wallet = st.wallet
    st.wallet = lambda: dummy_wallet  # type: ignore

    with set_block_number(1001):
        blocks.process_block_allowance_with_reporting(1001, supertensor_=st)

    # Sanity: confirm we have logically different hotkey now
    assert dummy_wallet.get_hotkey().ss58_address != old_hotkey

    # BUG: This call will raise NotEnoughAllowanceException (no miners) because query still filters
    # on old_hotkey cached at Allowance construction time.
    # DESIRED: Should return a non-empty list (at least one miner with allowance seconds > 0).
    miners = allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)

    # If code is fixed, we assert we got miners; currently this assertion FAILS (RED phase)
    assert miners, (
        "Expected miners with allowance for new validator hotkey, got empty list (stale cached hotkey)."
    )

    # Cleanup: ensure future tests can rebuild a fresh Allowance instance after fix
    from compute_horde_validator.validator.allowance import default as allowance_module

    allowance_module._allowance_instance = None
    st.wallet = original_wallet  # restore
