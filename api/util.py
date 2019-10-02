
def get_keys(start_datetime, end_datetime, interval, prefix):
    return ['%s:%s' % (prefix, dt.strftime('%H%M')) for dt in datetime_range(start_datetime, end_datetime, interval)]


def datetime_range(start, end, delta):
    current = start
    while current <= end:
        yield current
        current += delta
