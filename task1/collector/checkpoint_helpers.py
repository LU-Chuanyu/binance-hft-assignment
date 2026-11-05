"""生成可序列化的本地盘口前 20 档检查点。"""


def make_checkpoint(sync):
    """调用者须先确认当前快照和实时流已经完成对齐。"""
    if not sync.valid:
        raise ValueError('盘口无效，不能生成检查点')
    levels = sync.book.top_levels(20)
    return {'last_update_id': sync.last_update_id,
            'top': {side: [[str(price), str(quantity)] for price, quantity in levels[side]]
                    for side in ('bids', 'asks')}}
