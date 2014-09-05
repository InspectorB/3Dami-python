
# can't do tail recursion, so written as a loop
def sum_dicts(dicts, result=False):
    r = result if result else {}
    for d in dicts:
        for key in list(set(d.keys() + r.keys())):
            if type(d.get(key, {})) == dict \
                and type(r.get(key, {})) == dict:
                r[key] = sum_dicts([d.get(key, {})], r.get(key, {}))
            else:
                r[key] = r.get(key, 0) + d.get(key, 0)
    return r
