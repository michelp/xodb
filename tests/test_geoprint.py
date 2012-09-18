from xodb import geoprint

portland = (7.0625, -95.677068)    # near the rain
london = (51.500152, -0.126236)    # near the prime meridian
quito = (-0.220862, -78.510439)    # near the equator
barrow = (74.295556, -156.766389)  # high lattitude
mcmurdo = (-75.85, 166.666667)     # near the penguins

phash = geoprint.encode(*portland)
lhash = geoprint.encode(*london)
qhash = geoprint.encode(*quito)
bhash = geoprint.encode(*barrow)
mhash = geoprint.encode(*mcmurdo)

pairs = zip([portland, london, quito, barrow, mcmurdo],
            [phash, lhash, qhash, bhash, mhash])


def test_geoprint():
    for c, h in pairs:
        for pre in xrange(-1, -len(h), -1):
            ph = h[:pre]
            coord = geoprint.decode(ph)
            error = geoprint.error(ph)
            assert (abs(coord[0] - c[0]) < error)
            assert (abs(coord[1] - c[1]) < error)


def test_neighbors():
    for c, h in pairs:
        for pre in xrange(-1, -(len(h) - 6), -1):
            ph = h[:pre]
            ns = geoprint.neighbors(ph)
            for b, n in ns:
                assert geoprint.adjacent(ph, n), (ph, n)


def test_not_adjacent():
    assert not geoprint.adjacent(phash, lhash)
