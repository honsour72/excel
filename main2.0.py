import os
import pandas as pd
# import xlsxwriter


# ====================================================ФУНКЦИИ===========================================================


def get_non_game_row_zone(row):
    zones_list = list(row[["1H", "2H", "1Q", "2Q", "3Q", "4Q"]].values)
    for z in range(len(zones_list)-1, -1, -1):
        if zones_list[z][0] != '1' and zones_list[z][0] != '2' and zones_list[z][0] != '3' and zones_list[z][0] != '4':
            del zones_list[z]
    return zones_list


def samer(l1, l2):
    flag = 1
    for el in range(len(l1)):
        if l1[el] != l2[el]: flag = 0
    return flag


def filter2_0(df):
    def find_same_rows_indexes(row):
        index_list = []
        for r in range(row+1, len(ikdcv_list)):
            same = samer(ikdcv_list[r][1], ikdcv_list[row][1])
            if same:
                index_list.append(r)
        index_list.reverse()
        return index_list
    # Версия 3
    worked_df = df[["День", "Месяц", "Команда 1", "Команда 2"]]
    index_key_date_comands_value = {}
    for r in range(len(worked_df.index)):
        index_key_date_comands_value[r] = list(worked_df.loc[worked_df.index[r]].values)
    # ищем похожие строки
    ikdcv_list = list(index_key_date_comands_value.items())
    indexes_list = list(df.index)
    i = 0
    row = indexes_list[i]
    while row < indexes_list[-1]:
        same_index_list = find_same_rows_indexes(row)
        for same_index in same_index_list:
            # получаем инфу о зоне
            zones = get_non_game_row_zone(df.loc[same_index])
            # подставляем её в исходную строку
            df.loc[row, zones] = zones
            # удаляем найденную строку
            df = df.drop(same_index)
        indexes_list = list(df.index)
        i += 1
        row = indexes_list[i]
        if row > len(indexes_list): break
    # Версия 1
    # for game_zone_row in range(len(date_comands_from_game_zone_rows)-1):
    #     current_date_comands_from_game_zone = date_comands_from_game_zone_rows[game_zone_row]
    #     for non_game_zone_row in range(len(date_comands_from_non_game_zone_rows)-1, game_zone_row, -1):
    #         current_date_comands_from_non_game_zone = date_comands_from_non_game_zone_rows[non_game_zone_row]
    #         same_date_comands = samer(current_date_comands_from_game_zone, current_date_comands_from_non_game_zone)
    #         if same_date_comands:
    #             df = df.drop(df.index[non_game_zone_row])
    # Версия 2
    # date_comands_from_game_zone_rows = [list(d) for d in df.loc[
    #     df["Game"] == "Game", ["День", "Месяц", "Команда 1", "Команда 2"]].values]
    # date_comands_from_non_game_zone_rows = [list(d) for d in df.loc[
    #     df["Game"] != "Game", ["День", "Месяц", "Команда 1", "Команда 2"]].values]
    # game_len = len(date_comands_from_game_zone_rows)
    # for row_non_game in range(len(date_comands_from_non_game_zone_rows)):
    #     row_from_non_game = date_comands_from_non_game_zone_rows[row_non_game]
    #     for row_game in range(len(date_comands_from_game_zone_rows)):
    #         row_from_game = date_comands_from_game_zone_rows[row_game]
    #         same_result = samer(row_from_game, row_from_non_game)
    #         if same_result:
    #             zone = get_non_game_row_zone(df.loc[df.index[row_non_game]+game_len])
    #             df.loc[df.index[row_game], zone] = zone
    #             df = df.drop(df.index[row_non_game]+game_len)
    df = paste_game_data_from_game_sheet(month_sort(df))
    return df


def del_same_rows_in_one_zone_table(df):
    worked_df = df[["День", "Месяц", "Команда 1", "Команда 2"]]
    index_key_rows_to_del_value = {}
    for r in range(len(worked_df.index)):
        index_key_rows_to_del_value[r] = list(worked_df.loc[worked_df.index[r]].values)
    # ищем похожие строки
    ikrdv_list = list(index_key_rows_to_del_value.items())
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


def get_zone_column_name(answer_table):
    """
    Функция, которая принимает на вход отфильтрованную по определённой зоне таблицу (например Game)
    1) переименовывает столбец, в котором эта зона указана с "Unnamed: 1" на название зоны
    2) добавляет остальные зоны в правильном порядке
    """
    zone_name = list(answer_table["Unnamed: 1"].values)[0]
    answer_table = answer_table.rename(columns={"Unnamed: 1": zone_name})
    if zone_name == "Game":
        zones_columns = {"1H": 2, "2H": 3, "1Q": 4, "2Q": 5, "3Q": 6, "4Q": 7}
        for z, _ in list(zones_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if zone_name == "1H":
        zones_columns = {"Game": 1, "2H": 3, "1Q": 4, "2Q": 5, "3Q": 6, "4Q": 7}
        for z, _ in list(zones_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if zone_name == "2H":
        zones_columns = {"1H": 1, "Game": 1, "1Q": 4, "2Q": 5, "3Q": 6, "4Q": 7}
        for z, _ in list(zones_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if zone_name == "1Q":
        zones_columns = {"2H": 1, "1H": 1, "Game": 1, "2Q": 5, "3Q": 6, "4Q": 7}
        for z, _ in list(zones_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if zone_name == "2Q":
        zones_columns = {"1Q": 1, "2H": 1, "1H": 1, "Game": 1, "3Q": 6, "4Q": 7}
        for z, _ in list(zones_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if zone_name == "3Q":
        zones_columns = {"2Q": 1, "1Q": 1, "2H": 1, "1H": 1, "Game": 1, "4Q": 7}
        for z, _ in list(zones_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    if zone_name == "4Q":
        zones_columns = {"3Q": 1, "2Q": 1, "1Q": 1, "2H": 1, "1H": 1, "Game":1}
        for z, _ in list(zones_columns.items()):
            location = _
            column_name = z
            answer_table.insert(location, column_name, " ")

    return answer_table


def get_row_info(row):
    # получаем список из элементов строки из зоны
    # и если парочка его элементов равна nan, то возвращаем значение "пусто"
    if str(row[0]) == 'nan' or str(row[3]) == 'nan':
        return "empty"
    # иначе модернизируем наши условия: из 6 символов делаем 2 строки
    else:
        first_condition  = row[0] + " " + row[1] + " " + str(row[2])
        second_condition = row[3] + " " + row[4] + " " + str(row[5])
        return [first_condition, second_condition]


def get_all_conditions(all_columns):
    # Словарь со ВСЕМИ условиями из всех колонок из листа ФОРМУЛЫ
    all_conditions = {}
    # Проходимся по каждой колонке в словаре
    for column_title, column_frame in all_columns.items():
        # Создаём словарь зон для текущей колонки
        zones = {}
        # Проходимся по каждой строке в колонке
        for row in range(len(column_frame.index)):
            # получаем статус строки
            row_condition = get_row_info(list(column_frame.loc[row]))
            # если он не пустой ("empty") то в словарь зон добавляем наше условие, то есть row_condition
            # по ключу, полученному из первого столбца этой же строки
            if row_condition != "empty":
                # получаем ключ (зону) из 0 столбца по текшей строке
                # row_zone = get_zone_by_row(column_frame, row)
                # аналоговый вариант черех ранее полученный список индексов
                row_zone = all_zones[row]
                # и, если этого ключа в словаре зон нет - создаём его
                if not row_zone in zones:
                    zones[row_zone] = [row_condition]
                # а если - есть - добавляем по нему условие
                else:
                    zones[row_zone].append(row_condition)
                all_conditions[column_title] = zones
    return all_conditions


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


def paste_game_data_from_game_sheet(df):
    GAME = pd.read_excel(xlsx, sheet_name='Game').loc[1:]
    # rows_from_game_sheet = [list(i) for i in GAME[["День", "Месяц", "Команда 1", "Команда 2"]].values]
    rows_from_game_sheet = GAME[["День", "Месяц", "Команда 1", "Команда 2"]]
    """
    needed_rows_from_our_df = GAME.loc[, ['Результат',  'Unnamed: 7',  'Unnamed: 8',  'Unnamed: 9',
                                                            'Unnamed: 10',  'Unnamed: 11',  'Unnamed: 12',  
                                                            'Unnamed: 13',  'Unnamed: 14', 
                                                            'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10',
                                                            'Ф1', 'Ф2', 'ТБ', 'ТМ', 'Т1М', 'Т1Б', 'Т2Б', 'Т2М']]
    """
    # non_game_rows = [list(i) for i in df.loc[df["Game"] != "Game", ["День", "Месяц", "Команда 1", "Команда 2"]].values]
    non_game_rows = df.loc[df["Game"] != "Game", ["День", "Месяц", "Команда 1", "Команда 2"]]
    parse_info = ['Результат', 'Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9',
                                                      'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12',
                                                      'Unnamed: 13', 'Unnamed: 14',
                                                      'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10',
                                                      'Ф1', 'Ф2', 'ТБ', 'ТМ', 'Т1М', 'Т1Б', 'Т2Б', 'Т2М']
    for row in range(len(non_game_rows.index)):
        for r in range(len(rows_from_game_sheet.index)):
            our_row_list = list(non_game_rows.loc[non_game_rows.index[row]].values)
            row_list_from_game = list(rows_from_game_sheet.loc[rows_from_game_sheet.index[r]].values)
            result = samer(our_row_list, row_list_from_game)
            if result:
                needed_rows_from_our_df = GAME.loc[rows_from_game_sheet.loc[rows_from_game_sheet.index[r]].name,
                                                   parse_info]

                df.loc[non_game_rows.loc[non_game_rows.index[row]].name, parse_info] = needed_rows_from_our_df
    return df


# ======================================================================================================================


pd.set_option('display.max_rows', 70)
pd.set_option('display.max_columns', 80)

os.chdir("C:/Users/Михаил/Desktop/Python/Имена/Denis/фаза 2")
xlsx = [o for o in os.listdir() if o.endswith('.xlsx')][0]  # NBA.xlsx
# xlsx = [o for o in os.listdir() if o.endswith('.xlsx')][-1]  # NBA2.xlsx

FORMULA = pd.read_excel(xlsx, sheet_name='Формулы')


# =============================================ЭТАП 1. ПОЛУЧЕНИЕ УСЛОВИЙ================================================

processed = True

if processed:
    # Раздробить лист ФОРМУЛЫ на колонки
    FORA_1 = FORMULA[['Фора 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6']]
    FORA_2 = FORMULA[['Фора 2', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12']]
    TB  = FORMULA[['ТБ', 'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16', 'Unnamed: 17', 'Unnamed: 18']]
    TM  = FORMULA[['ТМ', 'Unnamed: 20', 'Unnamed: 21', 'Unnamed: 22', 'Unnamed: 23', 'Unnamed: 24']]
    T1M = FORMULA[['Т1М', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30']]
    T1B = FORMULA[['Т1Б', 'Unnamed: 32', 'Unnamed: 33', 'Unnamed: 34', 'Unnamed: 35', 'Unnamed: 36']]
    T2B = FORMULA[['Т2Б', 'Unnamed: 38', 'Unnamed: 39', 'Unnamed: 40', 'Unnamed: 41', 'Unnamed: 42']]
    T2M = FORMULA[['Т2М', 'Unnamed: 44', 'Unnamed: 45', 'Unnamed: 46', 'Unnamed: 47', 'Unnamed: 48']]

    # список всех зон по порядку для получения зоны по строке
    all_zones = list(FORMULA["Unnamed: 0"])

    # Словарь all_columns, в котором ключ - название колонки
    # (для записи в словарь условий all_conditions)
    # а значение - датафрейм этой колонки (для поиска условий)
    all_columns = {"Фора 1": FORA_1, "Фора 2": FORA_2, "ТБ": TB, "ТМ": TM, "Т1М": T1M, "Т1Б": T1B, "Т2Б": T2B, "Т2М": T2M}

    # получаем все условия из листа ФОРМУЛЫ
    all_conditions = get_all_conditions(all_columns)

    # вывод на печать всех условий для проверки
    vivod = True
    if vivod:
        for col, conds in all_conditions.items():
            print(col)
            for zone, all_conds in conds.items():
                print(zone, all_conds)

# ==============================================ЭТАП 2. ПРИМЕНЕНИЕ УСЛОВИЙ==============================================

processed = True

if processed:

    result = {}
    for result_sheet_name, sheet_conditions in all_conditions.items():
        answer_list_for_all_conditioned_tables = []
        for zone_name, all_zones_conditionals in sheet_conditions.items():
            zone_codition_tables_list = []
            work_sheet = pd.read_excel(xlsx, sheet_name=zone_name).loc[1:]
            for one_condition_list in all_zones_conditionals:
                one_condition = work_sheet.query(one_condition_list[0] + " & " + one_condition_list[1]).loc[:, "Сезон":"Т2М"]
                zone_codition_tables_list.append(one_condition)
            zone_condition = pd.concat(zone_codition_tables_list, ignore_index=True)
            zone_condition = zone_condition.rename(columns={"Дата": "День", "Unnamed: 3": "Месяц"})
            # вот тут удаляются повторения в одноимённых зонах
            zone_condition = del_same_rows_in_one_zone_table(zone_condition)
            zone_condition = get_zone_column_name(zone_condition)
            answer_list_for_all_conditioned_tables.append(zone_condition)
        result_dataframe = pd.concat(answer_list_for_all_conditioned_tables, ignore_index=True)
        result_dataframe = filter2_0(result_dataframe)
        result[result_sheet_name] = result_dataframe

    print(result)


# ==============================================ЭТАП 3. ЗАПИСЬ РЕЗУЛЬТАТОВ==============================================

processed = True

if processed:

    with pd.ExcelWriter('result.xlsx') as writer:
        for sheet_name, sheet_table in result.items():
            sheet_table.to_excel(writer, sheet_name=sheet_name)