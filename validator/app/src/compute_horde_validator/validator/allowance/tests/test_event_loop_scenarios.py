import asyncio
import threading
from unittest.mock import Mock, patch

import pytest

from compute_horde_validator.validator.allowance.utils.supertensor import make_sync


class TestNewEventLoopArchitecture:
    """Test the new dedicated background loop architecture."""
    
    def test_background_loop_architecture(self):
        """Test basic background loop architecture."""
        
        @make_sync
        async def simple_async_method(self):
            await asyncio.sleep(0.01)
            return "background_loop_success"
        
        # Mock SuperTensor with background loop
        mock_supertensor = Mock()
        
        # Set up a real background loop like SuperTensor would
        loop = None
        background_thread = None
        loop_ready = threading.Event()
        
        def loop_runner():
            nonlocal loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop_ready.set()
            try:
                loop.run_forever()
            finally:
                loop.close()
        
        background_thread = threading.Thread(target=loop_runner, daemon=True)
        background_thread.start()
        loop_ready.wait()
        
        # Attach loop to mock
        mock_supertensor.loop = loop
        
        # Test from sync context
        results = []
        exceptions = []
        
        def sync_thread_worker():
            try:
                # Verify no event loop in this thread
                with pytest.raises(RuntimeError):
                    asyncio.get_running_loop()
                
                # This should work - dispatches to background loop
                result = simple_async_method(mock_supertensor)
                results.append(result)
                
            except Exception as e:
                exceptions.append(e)
        
        thread = threading.Thread(target=sync_thread_worker)
        thread.start()
        thread.join()
        
        # Clean up background loop
        loop.call_soon_threadsafe(loop.stop)
        background_thread.join(timeout=1.0)
        
        assert not exceptions, f"Unexpected exceptions: {exceptions}"
        assert results == ["background_loop_success"]
    
    def test_async_context_guard_behavior(self):
        """Test that async contexts correctly raise guard errors."""
        
        @make_sync
        async def guarded_method(self):
            await asyncio.sleep(0.01)
            return "should_not_reach"

        async def async_caller():
            dummy_self = Mock()
            dummy_self.loop = None  # Simulate no background loop
            
            # This should raise our guard error
            with pytest.raises(RuntimeError) as exc_info:
                guarded_method(dummy_self)
            
            assert "cannot be called from async contexts" in str(exc_info.value)
            assert "sync_to_async" in str(exc_info.value)
            
            return "guard_behavior_verified"

        result = asyncio.run(async_caller())
        assert result == "guard_behavior_verified"
    
    def test_supertensor_init_works(self):
        """Test that SuperTensor initialization works without event loop conflicts."""
        
        def test_in_thread():
            # Import SuperTensor in thread scope
            from compute_horde_validator.validator.allowance.utils.supertensor import SuperTensor

            with patch('turbobt.Bittensor') as mock_bittensor_class:
                with patch('bt_ddos_shield.turbobt.ShieldedBittensor') as mock_shielded_class:
                    # Mock the bittensor instances
                    mock_bittensor = Mock()
                    mock_subnet = Mock()
                    mock_bittensor.subnet.return_value = mock_subnet
                    mock_bittensor_class.return_value = mock_bittensor
                    mock_shielded_class.return_value = mock_bittensor

                    # This should work without event loop errors
                    if hasattr(SuperTensor, '__orig__init__'):
                        # Use original init if conftest has mocked it
                        supertensor = SuperTensor.__new__(SuperTensor)
                        SuperTensor.__orig__init__(
                            supertensor,
                            network="finney",
                            netuid=12,
                            archive_network=None
                        )
                    else:
                        supertensor = SuperTensor(
                            network="finney", 
                            netuid=12,
                            archive_network=None
                        )

                    # Verify background loop was created
                    assert supertensor.loop is not None
                    assert supertensor._background_thread is not None
                    assert supertensor._background_thread.is_alive()

                    # Clean up
                    supertensor.close()

                    return "init_success"

        results = []
        exceptions = []
        
        def thread_worker():
            try:
                # Verify no event loop in this thread
                with pytest.raises(RuntimeError):
                    asyncio.get_running_loop()
                
                result = test_in_thread()
                results.append(result)
                
            except Exception as e:
                exceptions.append(e)

        thread = threading.Thread(target=thread_worker)
        thread.start()
        thread.join()

        assert not exceptions, f"Unexpected exceptions: {exceptions}"
        assert results == ["init_success"]
    
    def test_timeout_preservation(self):
        """Test that timeout behavior is preserved."""
        from compute_horde_validator.validator.allowance.utils.supertensor import (
            SuperTensorTimeout, DEFAULT_TIMEOUT
        )
        
        @make_sync
        async def slow_async_method(self):
            # Sleep longer than default timeout
            await asyncio.sleep(DEFAULT_TIMEOUT + 1)
            return "should_not_reach"

        # Mock SuperTensor with background loop
        mock_supertensor = Mock()
        
        # Set up a real background loop 
        loop = None
        background_thread = None
        loop_ready = threading.Event()
        
        def loop_runner():
            nonlocal loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop_ready.set()
            try:
                loop.run_forever()
            finally:
                loop.close()
        
        background_thread = threading.Thread(target=loop_runner, daemon=True)
        background_thread.start()
        loop_ready.wait()
        
        mock_supertensor.loop = loop
        
        # Test timeout from sync context
        results = []
        exceptions = []
        
        def sync_thread_worker():
            try:
                # Verify no event loop in this thread
                with pytest.raises(RuntimeError):
                    asyncio.get_running_loop()
                
                # This should timeout and raise SuperTensorTimeout
                with pytest.raises(SuperTensorTimeout):
                    slow_async_method(mock_supertensor)
                
                results.append("timeout_worked")
                
            except Exception as e:
                exceptions.append(e)
        
        thread = threading.Thread(target=sync_thread_worker)
        thread.start()
        thread.join()
        
        # Clean up background loop
        loop.call_soon_threadsafe(loop.stop)
        background_thread.join(timeout=1.0)
        
        assert not exceptions, f"Unexpected exceptions: {exceptions}"
        assert results == ["timeout_worked"]