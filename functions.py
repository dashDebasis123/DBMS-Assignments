import csv
import pandas as pd


def is_alphabetically_smaller(str1, str2):
    str1 = str1.upper()
    str2 = str2.upper()
    return str1 < str2


def is_alphabetically_bigger(str1, str2):
    str1 = str1.upper()
    str2 = str2.upper()
    return str1 > str2


def merge(arr1, arr2, indx, is_asc):
    m = len(arr1)
    n = len(arr2)
    arr3 = []

    i = 0
    j = 0
    while i < m and j < n:
        if is_asc == True:
            if is_alphabetically_smaller(arr1[i][indx], arr2[j][indx]):
                arr3.append(arr1[i])
                i += 1
            else:
                arr3.append(arr2[j])
                j += 1
        elif is_alphabetically_bigger(arr1[i][indx], arr2[j][indx]):
            arr3.append(arr1[i])
            i += 1
        else:
            arr3.append(arr2[j])
            j += 1
    while i < m:
        arr3.append(arr1[i])
        i += 1
    while j < n:
        arr3.append(arr2[j])
        j += 1
    return arr3

# Function to mergeSort 2 arrays


def merge_sort(arr, lo, hi, index, is_asc):
    if lo == hi:
        return [arr[lo]]
    mid = lo + (hi - lo) // 2
    arr1 = merge_sort(arr, lo, mid, index, is_asc)
    arr2 = merge_sort(arr, mid + 1, hi, index, is_asc)

    return merge(arr1, arr2, index, is_asc)


def open_csv_file(csvfile):
    with open(csvfile, 'r') as file_obj:
        redr_obj = csv.reader(file_obj)
        csv1_lst = list(redr_obj)
    return csv1_lst


def write_csv_file(csvList, newfilename):
    filename = newfilename
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(csvList)
    df = pd.read_csv(filename)
    print(df)


def find_common_attr(csv1_lst, csv_lst2):
    comn_coulm = []
    for i in csv1_lst[0]:
        for j in csv_lst2[0]:
            if i == j:
                csv1_attr_indx = csv1_lst[0].index(i)
                csv2_attr_indx = csv_lst2[0].index(j)
                comn_coulm.append(i)
    return (comn_coulm, csv1_attr_indx, csv2_attr_indx)


def parsing(string):  # sourcery skip: avoid-builtin-shadow
    string = string.replace(",", "")
    str = string.lower().split()
    print(str)

    slct = []
    frm = []
    where = []
    order = []
    strLen = len(str)
    i = 1
    frmIndx = str.index('from')
    whereIndx = str.index('where')
    ordrIndex = str.index("order")
    while i != frmIndx:
        slct.append(str[i])
        i += 1
    i += 1
    while i != whereIndx:
        frm.append(str[i])
        i += 1
    i += 1
    while i != ordrIndex:
        where.append(str[i])
        i += 1
    i += 1
    while i != strLen:
        order.append(str[i])
        i += 1
    return (slct, frm, where, order)


def crossProd(csv1, csv2):
    csv1_lst = open_csv_file(csv1)
    csv_lst2 = open_csv_file(csv2)

    field_lst = []
    rows_lst = []
    field_lst = csv1_lst[0] + csv_lst2[0]
    i, j = 1, 1
    while i != len(csv1_lst):
        j = 1
        while j != len(csv_lst2):
            rows_lst.append(csv1_lst[i] + csv_lst2[j])
            j += 1
        i += 1
    rows_lst.insert(0, field_lst)
    return (rows_lst)


def crossProd_with_attr(csv1, csv2, lst):
    # oprning both the csv file and storing the values in separate list for
    # further list operation
    csv1_lst = open_csv_file(csv1)
    csv_lst2 = open_csv_file(csv2)

    field_lst = []  # contains only attribute name
    rows_lst = []  # contains only attributes values
    field_lst = csv1_lst[0] + csv_lst2[0]
    # cross product of both the csv file is happening in the while loop (only attrbutes values)
    i, j = 1, 1
    while i != len(csv1_lst):
        j = 1
        while j != len(csv_lst2):
            rows_lst.append(csv1_lst[i] + csv_lst2[j])
            j += 1
        i += 1

    # inserting field list in rows list at 0 position
    rows_lst.insert(0, field_lst)
    # inserting the index of the given attributes in a list so further it will be helpful
    # for appending those attribute to another list before writing it to a csv file
    store_index = []
    i = 0
    while i != len(lst):
        j = 0
        while j != len(rows_lst[0]):
            if lst[i] == rows_lst[0][j]:
                store_index.append(rows_lst[0].index(lst[i]))
            j += 1
        i += 1

    # crossProd list stores only slected attribute column from the rows list
    n = len(rows_lst)
    # creating empty list of list of size 'n'
    crossProd_lst = [[] for _ in range(n)]
    j = 0
    while j != len(store_index):
        i = 0
        while i != len(rows_lst):
            crossProd_lst[i].append(rows_lst[i][store_index[j]])
            i += 1
        j += 1

# writing the cross product of two csv files with selected attribute into a new csv file
    return (crossProd_lst)


def naturlJoin(csv1, csv2, lst):
    csv1_lst = open_csv_file(csv1)
    csv_lst2 = open_csv_file(csv2)

    # finding the common attribute in two csv file

    comn_coulm, csv1_attr_indx, csv2_attr_indx = find_common_attr(
        csv1_lst, csv_lst2)

    if not comn_coulm:
        print("There is no common column in the two .csv files ")
        exit(0)
    # if both the common columns have same rows then it is storing into a list
    rows_lst = []
    i, j = 0, 0
    while i != len(csv1_lst):
        j = 0
        while j != len(csv_lst2):
            if csv1_lst[i][csv1_attr_indx] == csv_lst2[j][csv2_attr_indx]:
                rows_lst.append(csv1_lst[i] + csv_lst2[j])
            j += 1
        i += 1

    if ('*' in lst) and (len(lst) == 1):

        # finding the duplicate attribute from the list and storing its indices in the list to eleminate it further
        dupicateList_indices = []
        i = 0
        while i != len(rows_lst[0]) - 1:
            j = i + 1
            while j != len(rows_lst[0]):
                if rows_lst[0][i] == rows_lst[0][j]:
                    dupicateList_indices.append(j)
                j += 1
            i += 1
        # print("\n", dupicateList_indices)

        for i in rows_lst:
            j = 0
            while j != len(dupicateList_indices):
                i.pop(dupicateList_indices[j])
                j += 1

    elif ('*' not in lst) and (len(lst) >= 1):
        both = set(lst).intersection(rows_lst[0])
        indices_selctAttr = [rows_lst[0].index(x) for x in both]

        # naturlJoin_attr list stores only slected attribute column from the rows list
        n = len(rows_lst)
        # creating empty list of list of size 'n'
        naturlJoin_attr = [[] for _ in range(n)]
        j = 0
        while j != len(indices_selctAttr):
            i = 0
            while i != len(rows_lst):
                naturlJoin_attr[i].append(rows_lst[i][indices_selctAttr[j]])
                i += 1
            j += 1
        rows_lst = naturlJoin_attr
    else:
        print("SELECT clause has wrong input !!!")
        exit(0)

    return (rows_lst)


def selectStar(csvfile):
    return open_csv_file(csvfile) if '.csv' in csvfile else csvfile


def selectAttr(csv1, lst):
    csv1_lst = open_csv_file(csv1) if '.csv' in csv1 else csv1
    store_index = []
    i = 0
    while i != len(lst):
        j = 0
        while j != len(csv1_lst[0]):
            if lst[i] == csv1_lst[0][j]:
                store_index.append(csv1_lst[0].index(lst[i]))
            j += 1
        i += 1

    n = len(csv1_lst)
    rows_lst = [[] for _ in range(n)]
    j = 0
    while j != len(store_index):
        i = 0
        while i != len(csv1_lst):
            rows_lst[i].append(csv1_lst[i][store_index[j]])
            i += 1
        j += 1
    return rows_lst


def where_clause(csvfile, where_cond):

    csv_lst = open_csv_file(csvfile) if '.csv' in csvfile else csvfile
    new_csvlst = []
    if where_cond[0] in csv_lst[0]:
        attr_indx = csv_lst[0].index(where_cond[0])
        if '=' in where_cond:
            i = 1
            while i != len(csv_lst):
                if csv_lst[i][attr_indx] == where_cond[2]:
                    new_csvlst.append(csv_lst[i])
                i += 1

        elif '>' in where_cond:
            i = 1
            while i != len(csv_lst):
                if csv_lst[i][attr_indx] > where_cond[2]:
                    new_csvlst.append(csv_lst[i])
                i += 1

        elif '<' in where_cond:
            i = 1
            while i != len(csv_lst):
                if csv_lst[i][attr_indx] < where_cond[2]:
                    new_csvlst.append(csv_lst[i])
                i += 1

        elif '>=' in where_cond:
            i = 1
            while i != len(csv_lst):
                if csv_lst[i][attr_indx] >= where_cond[2]:
                    new_csvlst.append(csv_lst[i])
                i += 1

        elif '<=' in where_cond:
            i = 1
            while i != len(csv_lst):
                if csv_lst[i][attr_indx] <= where_cond[2]:
                    new_csvlst.append(csv_lst[i])
                i += 1

        elif '!=' in where_cond:
            i = 1
            while i != len(csv_lst):
                if csv_lst[i][attr_indx] != where_cond[2]:
                    new_csvlst.append(csv_lst[i])
                i += 1

        else:
            print("where operator is not correct , please check again!!")
            exit(0)

    else:
        print("no such attribute exists ,please check again")
        exit(0)

    if not new_csvlst:
        print("there is no such attribute values with given condition")
        exit(0)
    new_csvlst.insert(0, csv_lst[0])
    return (new_csvlst)


def order_clause(csvfile, orderList):
    csv_lst = open_csv_file(csvfile) if '.csv' in csvfile else csvfile
    is_asc = True
    if len(orderList) == 1 or orderList[1] == 'asc':
        pass
    elif orderList[1] == 'desc':
        is_asc = False
    else:
        print("Order manner is wrong. please mention ASC or DESC")

    if orderList[0] in csv_lst[0]:
        attr_indx = csv_lst[0].index(orderList[0])
        n = len(csv_lst)
        a = merge_sort(csv_lst, 1, n - 1, attr_indx, is_asc)

    else:
        print("given attribute name for order clause is not present ")
        exit(0)
    a.insert(0, csv_lst[0])
    return a
