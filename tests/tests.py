





a1 = {
    'a': 5,
    'b': {
        'c': {
            'd': 8
        }
    }
}


print a1


def try_change_deep_value(obj):
    value = obj
    keys = ['c', 'd']
    for k in keys:
        if k == keys[-1]:
            value[k] = 15
        value = value[k]





try_change_deep_value(a1['b'])


print a1