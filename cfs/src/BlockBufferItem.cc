#include "BlockBufferItem.h"

#include "FsProc_App.h"

ExportedBlockBufferItem::ExportedBlockBufferItem(BlockBufferHandle h)
    : ptr(h->getBufPtr()),
      block_no(h.get_key()),
      is_dirty(h->isDirty())
#ifdef DO_SCHED
      ,
      aid(h.get_tag().get_tenant()->get_app()->getAid())
#endif
{
}
