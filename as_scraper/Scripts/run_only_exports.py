#!/usr/bin/env python
import sys
sys.path.append('../')
from exports import *

## create a crawljob file for each one of the exports added next,
## check the description for each export
## in case we don't need any of them we can only comment it
print('running export: group_by_platform_id ...')
group_by_platform_id.execute()
print('running export: group_by_thread_id_with_single_page ...')
group_by_thread_id_with_single_page.execute()
print('running export: group_by_thread_id_with_multi_pages ...')
group_by_thread_id_with_multi_pages.execute()
print('DONE')