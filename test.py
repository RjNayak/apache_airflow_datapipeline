import random


def seq_gen(n):
    l = list(range(1, n+1))  # 1,2,3
    l.append(random.choice(l))  # 1,2,3,2
    random.shuffle(l)  # 1,2,2,3
    return l


def get_duplicated(l):
    num_dict = {}
    result = None

    for x in l:
        num_dict[x] = num_dict.get(x, 0)+1

    for key, val in num_dict.items():
        if val > 1:
            result = key

    return result


def test_function():
    lst = [2, 2]
    num = get_duplicated(lst)
    assert num == lst[0]


seq_ex = seq_gen(10)
print("Generated sequence : ", seq_ex)
duplicated_num = get_duplicated(seq_ex)

print(duplicated_num)
