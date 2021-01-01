import pandas as pd
import time
import datetime

# ====================================================ФУНКЦИИ===========================================================
# ЭТАП 1


def get_subcolumn_conditions(subcolumn):
    subcolumn = [list(i) for i in list(subcolumn.values)]
    for condition_list in range(len(subcolumn)-1, -1, -1):
        if str(subcolumn[condition_list][0]) == 'nan' and str(subcolumn[condition_list][3]) == 'nan':
            del subcolumn[condition_list]
        else:
            first_condition = subcolumn[condition_list][0] + " " +\
                          subcolumn[condition_list][1] + " " +\
                          str(subcolumn[condition_list][2])
            second_condition = subcolumn[condition_list][3] + " " +\
                           subcolumn[condition_list][4] + " " +\
                           str(subcolumn[condition_list][5])
            subcolumn[condition_list] = [first_condition, second_condition]
    return subcolumn


def get_all_conditions2(all_rows):
    # Словарь со ВСЕМИ условиями из всех строк из листа ФОРМУЛЫ
    all_conditions2 = {}
    # Проходимся по каждой колонке в словаре
    for column_title, column_frame in all_rows.items():
        # Создаём словарь зон для текущей колонки
        subcolumn = {"Ф1": [], "Ф2": [], "ТБ": [], "TM": [], "T1M": [], "Т1Б": [], "Т2Б": [], "T2M": []}
        # Проходимся по каждому столбцу в колонке
        # for i in range(0, len(list(column_frame.columns)), 6):
        for i in range(len(list(column_frame.columns))-1, -1, -6):
            # subcond = get_subcolumn_conditions(column_frame.loc[:, column_frame.columns[i]:column_frame.columns[i - 5]])
            subcond = get_subcolumn_conditions(column_frame.loc[:, column_frame.columns[i-5]:column_frame.columns[i]])
            if subcond:
                subcolumn[list(subcolumn.keys())[i // 6]] = subcond
            else:
                del subcolumn[list(subcolumn.keys())[i // 6]]
        if len(subcolumn) != 0:
            all_conditions2[column_title] = subcolumn
    return all_conditions2


# ЭТАП 2


def main_filter(all_conditions):
    # Cловарь с парой "название листа": датафрейм для записи в лист эксель
    result = {}
    # проходим по всем названиям листов и спискам с условиями:
    for result_sheet_name, sheet_conditions in all_conditions.items():
        # список, в который потом попадут все датафреймы (после применения нескольких условий к листам) для объединения
        # в итоговый датафрейм
        answer_list_for_all_conditioned_dataframes = []
        # проходимся по условиям каждого столбца для того, чтобы сначала применить все эти условия к листу
        # по названию result_sheet_name и получить один датафрейм за зону
        # а потом вставить в этот датафрейм столбец по имени col_name
        for column_name, all_columns_conditionals in sheet_conditions.items():
            # список, в который попадут датафреймы, полученные после применения ОДНОГО условия к одноименному листу
            # для объединения в общий датафрейс за всю зону
            column_codition_dataframes_list = []
            # читаем лист, название которого совпадает с названием текущей зоны
            work_sheet = all_dataframes[result_sheet_name]
            # посписочно берём список с условиями
            for one_condition_list in all_columns_conditionals:
                # применяем условия к соответствующему листу
                one_condition = work_sheet.query(one_condition_list[0] + " & " + one_condition_list[1]).loc[:,
                                "Сезон":"Т2М"]
                # добавляем полученный датафрейм в список, который создан к текущей зоне
                column_codition_dataframes_list.append(one_condition)
            # объединяем все элементы списка в один датафрейм
            column_condition_dataframe = pd.concat(column_codition_dataframes_list, ignore_index=True)
            # перименовывать столбец "Дата" в "День" и т.д. в данном случае не нужно (это не сделано изначально)
            column_condition_dataframe = column_condition_dataframe.rename(columns={"Дата": "День", "Unnamed: 3": "Месяц"})
            # вот тут удаляются повторения в одноимённых зонах
            column_condition_dataframe = del_same_rows_in_one_zone_table(column_condition_dataframe)
            # сначала вставим столбец с именем колнки, откуда пришли условия
            # column_condition_dataframe.insert(2, str(column_name)+" ", str(column_name)+" ")
            # а теперь добавляем колонки оставшиеся (Ф1 Ф2 ТБ ТМ Т1М Т1Б Т2Б Т2М)
            # а теперь все колонки (Ф1 Ф2 ТБ ТМ Т1М Т1Б Т2Б Т2М)
            column_condition_dataframe = get_zone_column_name(column_condition_dataframe, column_name)
            # добавляем итоговый (за одну зону) датафрейм в итоговый общий список из которого позже будет создан
            # итоговый датафрейм
            answer_list_for_all_conditioned_dataframes.append(column_condition_dataframe)
        # создаём тот самый итоговый датафрейм
        # 1ый метод: pd.concat (не работает корректно)
        result_dataframe = pd.concat(answer_list_for_all_conditioned_dataframes, ignore_index=True)
        # print(result_dataframe)
        # 2ой метод: присоединение из списка
        # result_dataframe = pd.DataFrame()
        # for frame in answer_list_for_all_conditioned_dataframes:
        #     result_dataframe = result_dataframe.append(frame)
        # вот эта та самая перестановка

        columns = list(result_dataframe.columns)
        new_cols = columns[0:2] + columns[-8:] + columns[2:-8]
        # print(new_cols)
        result_dataframe = result_dataframe[new_cols]
        # фильтруем на наличие повторяющихся строк (и не только)
        # result_dataframe = name_cols(result_dataframe)
        # new_result_dataframe = filter2_0(new_result_dataframe)

        # добавляеи в итоговый словарь
        result_dataframe = month_sort(result_dataframe)

        # вот это только что добавил
        # result_dataframe = del_same_rows_in_one_zone_table(result_dataframe)

        result_dataframe = filter_4(result_dataframe)

        result[result_sheet_name] = result_dataframe

    return result


def del_same_rows_in_one_zone_table(df):
    worked_df = df[["День", "Месяц", "Команда 1", "Команда 2"]]
    df = df.drop_duplicates(subset=["День", "Месяц", "Команда 1", "Команда 2"])
    return df


"""
def del_same_rows_in_one_zone_table(df):
    worked_df = df[["День", "Месяц", "Команда 1", "Команда 2"]]
    index_key_rows_to_del_value = {}
    for r in range(len(worked_df.index)):
        index_key_rows_to_del_value[r] = list(worked_df.loc[worked_df.index[r]].values)
    # ищем похожие строки
    # ikrdv_list = list(index_key_rows_to_del_value.items())
    ikrdv_list = [list(el) for el in list(index_key_rows_to_del_value.items())]
    def find_same_rows_indexes(row):
        index_list = []
        for r in range(row+1, len(ikrdv_list)):
            same = samer(ikrdv_list[r][1], ikrdv_list[row][1])
            if same:
                index_list.append(r)
        index_list.reverse()
        return index_list
    indexes_list = list(df.index)
    i = 0
    row = indexes_list[i]
    while row < indexes_list[-1]:
        same_index_list = find_same_rows_indexes(row)
        for same_index in same_index_list:
            # # получаем инфу о зоне
            # zones = get_non_game_row_zone(df.loc[same_index])
            # # подставляем её в исходную строку
            # df.loc[row, zones] = zones
            # # удаляем найденную строку
            df = df.drop(same_index)
        indexes_list = list(df.index)
        i += 1
        row = indexes_list[i]
        if row > len(indexes_list): break
    return df
"""


def get_zone_column_name(answer_table, column_name):
    answer_table[column_name+" "] = column_name + " "
    other_columns = ["Ф1 ", "Ф2 ", "ТБ ", "TM ", "T1M ", "Т1Б ", "Т2Б ", "T2M "]
    del other_columns[other_columns.index(column_name+" ")]
    for c in other_columns:
        answer_table[c] = ""
    return answer_table

'''
def get_zone_column_name(answer_table, column_name):
    """
    Функция, которая принимает на вход отфильтрованную по определённой зоне таблицу (например Game)
    1) переименовывает столбец, в котором эта зона указана с "Unnamed: 1" на название зоны
    2) добавляет остальные зоны в правильном порядке
    """
    if column_name == "Ф1":
        # answer_table.insert(2, column_name, column_name)
        # answer_table[column_name] = column_name
        # zone_name = list(answer_table["Unnamed: 1"].values)[0]
        # answer_table = answer_table.rename(columns={"Unnamed: 1": zone_name})
        # if column_name == "Ф1":
        other_columns = {"Ф2 ": 3, "ТБ ": 4, "ТМ ": 5, "Т1М ": 6, "Т1Б ": 7, "Т2Б ": 8, "T2M ": 9}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if column_name == "Ф2":
        # answer_table.insert(2, column_name, column_name)
        other_columns = {"Ф1 ": 2, "ТБ ": 4, "ТМ ": 5, "Т1М ": 6, "Т1Б ": 7, "Т2Б ": 8, "T2M ": 9}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if column_name == "ТБ":
        # answer_table.insert(2, column_name, column_name)
        other_columns = {"Ф2 ": 2, "Ф1 ": 2, "ТМ ": 5, "Т1М ": 6, "Т1Б ": 7, "Т2Б ": 8, "T2M ": 9}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if column_name == "TM":
        # answer_table.insert(2, column_name, column_name)
        other_columns = {"ТБ ": 2, "Ф2 ": 2, "Ф1 ": 2, "Т1М ": 6, "Т1Б ": 7, "Т2Б ": 8, "T2M ": 9}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if column_name == "T1M":
        # answer_table.insert(2, column_name, column_name)
        other_columns = {"ТМ ": 2, "ТБ ": 2, "Ф2 ": 2, "Ф1 ": 2, "Т1Б ": 7, "Т2Б ": 8, "T2M ": 9}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if column_name == "Т1Б":
        # answer_table.insert(2, column_name, column_name)
        other_columns = {"Т1М ": 2, "ТМ ": 2, "ТБ ": 2, "Ф2 ": 2, "Ф1 ": 2, "Т2Б ": 8, "T2M ": 9}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if column_name == "Т2Б":
        # answer_table.insert(2, column_name, column_name)
        other_columns = {"Т1Б ": 2, "Т1М ": 2, "ТМ ": 2, "ТБ ": 2, "Ф2 ": 2, "Ф1 ": 2, "T2M ": 9}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if column_name == "T2M":
        # answer_table.insert(2, column_name, column_name)
        other_columns = {"Т2Б ": 2, "Т1Б ": 2, "Т1М ": 2, "ТМ ": 2, "ТБ ": 2, "Ф2 ": 2, "Ф1 ": 2}
        for z, _ in list(other_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    return answer_table
'''


def filter_4(df):
    workzones = [list(el) for el in list(df.loc[:, 'День':"Команда 2"].values)]
    all_dataframes = []
    j = 0
    game = workzones[j]
    # for game in workzones:
    while game != workzones[-1]:
        new_df = df[(df['День'] == game[0])      & (df['Месяц'] == game[1]) &
                    (df['Команда 1'] == game[2]) & (df['Команда 2'] == game[3])]

        # if len(new_df.index) == 1:
        #	all_dataframes.append(new_df)
        # else:
        for row in range(len(new_df) - 1, 0, -1):
            needed_data = new_df.loc[new_df.index[row], 'Ф1 ':'T2M '].to_list()
            for el in range(len(needed_data) - 1, -1, -1):
                if needed_data[el] == '' or needed_data[el] == ' ' or str(needed_data[el]) == 'nan':
                    del needed_data[el]
            new_df.loc[new_df.index[0], needed_data] = needed_data
            df = df.drop(new_df.index[row])
        to_append = pd.DataFrame(new_df.loc[new_df.index[0]]).transpose()
        all_dataframes.append(to_append)
        workzones = [list(el) for el in list(df.loc[:, 'День':"Команда 2"].values)]
        j += 1
        game = workzones[j]
    result = pd.concat(all_dataframes, ignore_index=True)
    # return all_dataframes
    return result


def samer(l1, l2):
    """
    сравниватель 2 списков
    :param l1: список первый
    :param l2: список второй
    :return: команду на удаление
    """
    flag = 1
    for el in range(len(l1)):
            if l1[el] != l2[el]: flag = 0
    return flag


def month_sort(df):
    new10 = df.query("Месяц == 10")
    new10_day = new10.sort_values('День')
    new11 = df.query("Месяц == 11")
    new11_day = new11.sort_values('День')
    new12 = df.query("Месяц == 12")
    new12_day = new12.sort_values('День')
    new1  = df.query("Месяц == 1")
    new1_day = new1.sort_values('День')
    new2  = df.query("Месяц == 2")
    new2_day = new2.sort_values('День')
    new3 = df.query("Месяц == 3")
    new3_day = new3.sort_values('День')
    new4 = df.query("Месяц == 4")
    new4_day = new4.sort_values('День')
    new5 = df.query("Месяц == 5")
    new5_day = new5.sort_values('День')
    new6 = df.query("Месяц == 6")
    new6_day = new6.sort_values('День')
    new7 = df.query("Месяц == 7")
    new7_day = new7.sort_values('День')
    new8 = df.query("Месяц == 8")
    new8_day = new8.sort_values('День')
    new9 = df.query("Месяц == 9")
    new9_day = new9.sort_values('День')
    all_new = [new10_day, new11_day, new12_day, new1_day,new2_day, new3_day, new4_day, new5_day, new6_day, new7_day, new8_day, new9_day]
    df = pd.concat(all_new, ignore_index=True)
    return df


# =======================================ЧТЕНИЕ ФАЙЛОВ И ЗАПИСЬ ИХ В ДАТАФРЕЙМ==========================================


# pd.set_option('display.max_rows', 350)
pd.set_option('display.max_columns', 50)
# f = pd.ExcelFile("Формулы2.xlsx")
# f = pd.ExcelFile('C:\\Users\\rogoz\\Рабочий стол\\Python\\Имена\\Denis\\Часть 2\\Формулы.xlsx')
# form = f.parse('600')
# ЛИСТ С ФОРМУЛАМИ (УСЛОВИЯМИ)
print("Чтение листа с формулами . . .")
FORMULA = pd.read_excel("Формулы.xlsx", sheet_name='600').loc[:, "Фора 1":]
print("Чтение листа с формулами - ЗАВЕРШЕНО!")


# =============================================ЭТАП 1. ПОЛУЧЕНИЕ УСЛОВИЙ================================================

# ЗОНЫ С УСЛОВИЯМИ ДЛЯ КОНЕЧНОЙ ЗАПИСИ В ФАЙЛ ПО ЛИСТАМ С НАЗВАНИЯМИ ЭИХ ЗОН (ЗАДАНЫ ЖЁСТКО, НЕОБХОДИМО ПОМЕНЯТЬ)
GAME_ = FORMULA.iloc[0:10]
H1 = FORMULA.iloc[10:20]
H2 = FORMULA.iloc[20:30]
Q1 = FORMULA.iloc[30:40]
Q2 = FORMULA.iloc[40:50]
Q3 = FORMULA.iloc[50:60]
Q4 = FORMULA.iloc[60:70]

all_rows = {'Game': GAME_, '1H': H1, '2H': H2, '1Q': Q1, '2Q': Q2, '3Q': Q3, '4Q': Q4}

# ЧТЕНИЕ ОСТАЛЬНЫХ ЛИСТОВ (ВОТ ВСЯ ПРОГРАММА РАБОТАЕТ ДОЛГО ИМЕННО ИЗ-ЗА ЧТЕНИЯ ЭТИХ ЛИСТОВ)
all_dataframes = {}

print("Этап первый. \n Получение введённых условий из листа 'Формулы' . . .")
all_conditions = get_all_conditions2(all_rows)
print("На чтение открываются только те листы, которые были обнаружены в листе Формулы в одноимённых зонах")
for i in all_conditions:
    # print("Чтение только задёствованных в формулах листов из книги NBA.xlsx . . .")
    print("Чтение листа {} . . .".format(i))
    name = pd.read_excel("NBA.xlsx", sheet_name=i).loc[1:, "Сезон":"Т2М"]
    print("Чтение листа {} - завершено ✔".format(i))
    all_dataframes[i] = name



print("Полученные из листа Формулы условия:")
## ВЫВОД ПОЛУЧЕННЫХ УСЛОВИЙ НА ПЕЧАТЬ
for k, v in all_conditions.items():
    print(k)
    for k1, v1 in v.items():
        print(k1, v1)
print("Этап первый - ЗАВЕРШЁН")

# ==============================================ЭТАП 2. ПРИМЕНЕНИЕ УСЛОВИЙ==============================================

print("Этап второй. \n Применение полученных условий к таблицам . . .")
result = main_filter(all_conditions)
print("Этап второй - ЗАВЕРШЁН \n Полученные таблицы")
print(result)

# ==============================================ЭТАП 3. ЗАПИСЬ РЕЗУЛЬТАТОВ==============================================

print("Этап третий. \n Запись полученных отфильрованных таблиц в файл excel")
dn = str(datetime.datetime.now()).split(' ')[1].split(".")[0].replace(":", ".")
with pd.ExcelWriter("Результат" + " " + dn + ".xlsx") as writer:
    for sheet_name, sheet_table in result.items():
        sheet_table.to_excel(writer, sheet_name=sheet_name)
print("Этап третий - ЗАВЕРШЁН. \n РАБОТА ПРОГРАММЫ ВЫПОЛНЕНА УСПЕШНО")
