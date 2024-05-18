from unittest import mock

import pytest



# 0. in all of these check systemevents
# 1. all ok - insert some jobs, check that weight setting is called with the right values
# 2. subtensor connectivity error in first attempt
# 3. subtensor connectivity error in second attempt
# 4. metagraph fetching error
# 5. setting weight throws an error
# 6. setting weight times out
# 7. setting weights fails