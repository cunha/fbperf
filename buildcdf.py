def buildcdf(iterator, height_step=0.00001):
    result = list()

    last, cnt = next(iterator)
    if last is None:
        return [[0.0, 0.0], [0.0, 1.0]]

    val, weight = next(iterator)
    while val is not None:
        if val != last:
            assert not result or last > result[-1][0], 'input not sorted'
            result.append((last, cnt))
            cnt += weight
        else:
            cnt += weight
        last = val
        val, weight = next(iterator)

    if not result:
        result.append([0.0, 0.0])
    result.append([last, cnt])

    cdf = list()

    h = height_step
    i = 0
    while i < len(result):
        x, y = result[i][0], float(result[i][1])/cnt
        while y < h:
            i += 1
            x, y = result[i][0], float(result[i][1])/cnt
        cdf.append((x, y))
        while y >= h:
            h += height_step
        i += 1
    cdf.append((result[-1][0], 1.0))
    return cdf
