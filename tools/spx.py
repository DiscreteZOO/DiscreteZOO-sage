from sage.categories.cartesian_product import cartesian_product
from sage.rings.finite_rings.integer_mod_ring import IntegerModFactory as Integers
from graphzoo.zoograph import unique_id

def spx_adj(x, y):
    xv, xn, xs = x
    yv, yn, ys = y
    if xs == ys:
        return False
    if xn == yn:
        return xv == yv
    if xn + xs != yn:
        return False
    if xs == 1:
        return xv[1:] == yv[:-1]
    else:
        return yv[1:] == xv[:-1]

max = 1280
spx = {}
for r in range(3, max/4+1):
    spx[r] = {}
    s = 1
    n = 4*r
    while n <= max:
        c = [tuple(x) for x
             in cartesian_product([[tuple(y) for y in Integers(2)^s],
                                   Integers(r), [1, -1]])]
        spx[r][s] = Graph([c, spx_adj])
        s += 1
        n *= 2
    print "Finished r = %d, constructed %d graphs" % (r, len(spr[r]))

spx_uid = {}
for r, d in spx.items():
    spx_uid[r] = {}
    for s, G in d.items():
        spx_uid[r][s] = unique_id(G)
    print "Finished r = %d" % r
