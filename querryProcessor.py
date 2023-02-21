"""Name = Debasis Dash
    Regd no- 22556
    
    The following program is simple querry processing using csv file ,
    all the operation has been done in list"""

import csv
from functions import *
import pandas as pd


def querry_process(string):

    slct, frm, where, order = parsing(string)
    naturalJoin_present = False
    if 'naturaljoin' in frm:
        naturalJoin_present = True
        frm.remove("naturaljoin")

    print("query=\n", string)
    print("select= ", slct)
    print("from= ", frm)
    print("where= ", where)
    print("order= ", order)

    if len(frm) == 1:  # for one file

        if len(where) == 0 and len(order) == 0:

            if '*' in slct and len(slct) == 1:
                final_csvLst = selectStar(frm[0])
                new_csv = "1fil_selectStar.csv"

            elif '*' not in slct:
                final_csvLst = selectAttr(frm[0], slct)
                new_csv = "1file_selctAttr.csv"

            else:
                print("error in one file where = 0 order = 0")
                exit(0)

        elif len(where) == 0 and len(order) != 0:
            csvList = order_clause(frm[0], order)
            if '*' in slct and len(slct) == 1:
                final_csvLst = selectStar(csvList)
                new_csv = "1fil_ordr_selectStar.csv"

            elif '*' not in slct:
                final_csvLst = selectAttr(csvList, slct)
                new_csv = "1file_ordr_selctAttr.csv"

            else:
                print("error in one file where = 0 order = 1")
                exit(0)

        elif len(where) != 0 and len(order) == 0:
            csvList = where_clause(frm[0], where)

            if '*' in slct and len(slct) == 1:
                final_csvLst = selectStar(csvList)
                new_csv = "1fil_whr_selectStar.csv"

            elif '*' not in slct:
                final_csvLst = selectAttr(csvList, slct)
                new_csv = "1file_whr_selctAttr.csv"

            else:
                print("error in one file where = 1 order = 0")
                exit(0)

        elif len(where) != 0 and len(order) != 0:
            csvList = where_clause(frm[0], where)
            csvList2 = order_clause(csvList, order)

            if '*' in slct and len(slct) == 1:
                final_csvLst = selectStar(csvList)
                new_csv = "1fil_whr_ordr_selectStar.csv"

            elif '*' not in slct:
                final_csvLst = selectAttr(csvList2, slct)
                new_csv = "1file_whr_ordr_selctAttr.csv"

            else:
                print("error in one file where = 1 order = 1")
                exit(0)

        else:
            print('error in len 1')
            exit(0)

        write_csv_file(final_csvLst, new_csv)
    elif len(frm) == 2:  # for two file
        if naturalJoin_present == True:
            csvList = naturlJoin(frm[0], frm[1], slct)
            if len(where) == 0 and len(order) == 0:

                if '*' in slct and len(slct) == 1:
                    final_csvLst = selectStar(csvList)
                    new_csv = "2fil_natrl_selectStar.csv"

                elif '*' not in slct:
                    final_csvLst = selectAttr(csvList, slct)
                    new_csv = "1file_natrl_selctAttr.csv"

                else:
                    print("error in two file natural join, where = 0 order = 0")
                    exit(0)

            elif len(where) == 0 and len(order) != 0:
                csvList2 = order_clause(csvList, order)
                if '*' in slct and len(slct) == 1:
                    final_csvLst = selectStar(csvList2)
                    new_csv = "2fil_natrl_ordr_selectStar.csv"

                elif '*' not in slct:
                    final_csvLst = selectAttr(csvList2, slct)
                    new_csv = "2file_natrl_ordr_selctAttr.csv"

                else:
                    print("error in two file natural join, where = 0 order = 1")
                    exit(0)

            elif len(where) != 0 and len(order) == 0:
                csvList2 = where_clause(csvList, where)
                if '*' in slct and len(slct) == 1:
                    final_csvLst = selectStar(csvList2)
                    new_csv = "2fil_natrl_whr_selectStar.csv"

                elif '*' not in slct:
                    final_csvLst = selectAttr(csvList2, slct)
                    new_csv = "2file_natrl_whr_selctAttr.csv"

                else:
                    print("error in two file natural join where = 1 order = 0")
                    exit(0)

            elif len(where) != 0 and len(order) != 0:
                csvList2 = where_clause(csvList, where)
                csvList3 = order_clause(csvList2, order)
                if '*' in slct and len(slct) == 1:
                    final_csvLst = selectStar(csvList3)
                    new_csv = "2fil_natrl_whr_ordr_selectStar.csv"

                elif '*' not in slct:
                    final_csvLst = selectAttr(csvList3, slct)
                    new_csv = "1file_natrl_whr_ordr_selctAttr.csv"

                else:
                    print("error in two file natural join where = 1 order = 1")
                    exit(0)

            else:
                print('error in len 2 natural join')
                exit(0)
            write_csv_file(final_csvLst, new_csv)

        elif naturalJoin_present == False:  # cross product with attribute

            csvList1 = crossProd_with_attr(frm[0], frm[1], slct)
            csvList2 = crossProd(frm[0], frm[1])
            if len(where) == 0 and len(order) == 0:

                if '*' in slct and len(slct) == 1:
                    final_csvLst = csvList2
                    new_csv = "2fil_cross_selectStar.csv"

                elif '*' not in slct:

                    final_csvLst = csvList1
                    new_csv = "2file_cross_attr_selctAttr.csv"

                else:
                    print("error in two file cross product, where = 0 order = 0")
                    exit(0)

            elif len(where) == 0 and len(order) != 0:

                if '*' in slct and len(slct) == 1:
                    csvList3 = order_clause(csvList2, order)
                    final_csvLst = selectStar(csvList3)
                    new_csv = "2fil_cross_ordr_selectStar.csv"

                elif '*' not in slct:
                    csvList3 = order_clause(csvList1, order)
                    final_csvLst = selectAttr(csvList3, slct)
                    new_csv = "2file_cross_ordr_selctAttr.csv"

                else:
                    print("error in two file cross prod, where = 0 order = 1")
                    exit(0)

            elif len(where) != 0 and len(order) == 0:

                if '*' in slct and len(slct) == 1:
                    csvList3 = where_clause(csvList2, where)
                    final_csvLst = selectStar(csvList3)
                    new_csv = "2fil_natrl_whr_selectStar.csv"

                elif '*' not in slct:
                    csvList3 = where_clause(csvList1, where)
                    final_csvLst = selectAttr(csvList3, slct)
                    new_csv = "2file_natrl_whr_selctAttr.csv"

                else:
                    print("error in two file cross prod where = 1 order = 0")
                    exit(0)

            elif len(where) != 0 and len(order) != 0:

                if '*' in slct and len(slct) == 1:
                    csvList3 = where_clause(csvList2, where)
                    csvList4 = order_clause(csvList3, order)
                    final_csvLst = selectStar(csvList4)
                    new_csv = "2fil_natrl_whr_ordr_selectStar.csv"

                elif '*' not in slct:
                    csvList3 = where_clause(csvList1, where)
                    csvList4 = order_clause(csvList3, order)
                    final_csvLst = selectAttr(csvList4, slct)
                    new_csv = "1file_natrl_whr_ordr_selctAttr.csv"

                else:
                    print("error in two file cross prod where = 1 order = 1")
                    exit(0)
            write_csv_file(final_csvLst, new_csv)
        else:
            print('error in len 2 cross prod')
            exit(0)
    else:
        print("error- from has no csv file or more than 2 csv file with attributes")


def main():
    print("querry1=SELECT calories FROM data.csv, addresses.csv  WHERE pulse > 1 ORDER calories desc,")
    print("query2 = SELECT calories FROM data.csv, addresses.csv  where order")
    print("query is for only one or two csv file")
    query = input(
        "Give a query, if you are not using where or order clause still you should mention it like given example\n")
    querry_process(query)


main()
