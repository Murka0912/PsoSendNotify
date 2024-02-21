import xlwt
import os
def exportDataxls(rows, date, newdate, rows1):


    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('Исходящая корреспондениция ПСО')

    # Sheet header, first row
    new_row_num = 0
    borders = xlwt.Borders()
    borders.bottom = 1
    borders.left = 1
    borders.right = 1
    borders.top = 1
    borders.bottom_colour = 0
    borders.right_colour = 0
    borders.left_colour = 0
    borders.top_colour = 0
    font_style = xlwt.XFStyle()
    font = xlwt.Font()
    font.bold = True
    font_style.font = font

    style_string = "font: bold on; borders: bottom 1; borders: left 1; borders: right 1; borders: top 1"
    style = xlwt.easyxf(style_string)

    columns = ['Статус выгрузки','Отправитель','Получатель','Номер документа','Текущий статус пакета','Дата и время постановки в очередь']
    font_style = xlwt.XFStyle()

    font_style.borders = borders
    columns_count = ['Количество пакетов', 'статус']
    for col_num1 in range(len(columns_count)):
        ws.write(new_row_num, col_num1, columns_count[col_num1], style)
        cells = xlwt.Column(col_num1, ws)
        cells.width = 500
    summa = 0
    for row1 in rows1:
        ro1 = list(row1)
        new_row_num += 1

        if ro1[1]== None:

            ro1[1] = "Ошибка выгрузки"

        b = [row1[0],ro1[1]]

        summa = summa +row1[0]
        ws.write(new_row_num, 0, b[0], style)
        ws.write(new_row_num, 1, b[1], font_style)
    ws.write(new_row_num+1, 0, summa, style)
    ws.write(new_row_num+1, 1, 'Всего пакетов', font_style)
    row_num = new_row_num + 3
    for col_num in range(len(columns)):
        ws.write(row_num, col_num, columns[col_num], style)

    # Sheet body, remaining rows
        cells = xlwt.Column(col_num,ws)
        cells.width = 5000

    for row in rows:
        row_num += 1
        a = [row[0],row[1],row[2],row[3],row[4], str(row[5]) ]

        for col_num in range(len(a)):
            ws.col(0).width = 30 * 256
            ws.col(1).width = 100 * 256
            ws.col(2).width = 30 * 256
            ws.col(3).width = 30 * 256
            ws.col(4).width = 30 * 256
            ws.col(5).width = 30 * 256
            ws.write(row_num, col_num, a[col_num], font_style)
        #
        #ws.write(row_num, , [[row.srvaddr],[row.namedisk], [str(row.freespace)],[str(row.allspace)],[row.date_up]], font_style)
    print('start adding new table', row_num)

    try:
        os.mkdir(".\\reports\\")
        wb.save(f".\\reports\\Отчет ПСО отправленные док-ты-(с {newdate.strftime('%Y-%m-%d')} по {date.strftime('%Y-%m-%d')}).xls")
    except: print('ALready')
def create_report_APE(rows, date):


    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('Документы без резолюции')

    # Sheet header, first row
    new_row_num = 0
    borders = xlwt.Borders()
    borders.bottom = 1
    borders.left = 1
    borders.right = 1
    borders.top = 1
    borders.bottom_colour = 0
    borders.right_colour = 0
    borders.left_colour = 0
    borders.top_colour = 0
    font_style = xlwt.XFStyle()
    font = xlwt.Font()
    font.bold = True
    font_style.font = font

    style_string = "font: bold on; borders: bottom 1; borders: left 1; borders: right 1; borders: top 1"
    style = xlwt.easyxf(style_string)

    columns = ['Наименование журнала регистрации','рег. дата документа','рег. номсер документа']
    font_style = xlwt.XFStyle()

    font_style.borders = borders

    row_num = new_row_num
    for col_num in range(len(columns)):
        ws.write(row_num, col_num, columns[col_num], style)

    # Sheet body, remaining rows
        cells = xlwt.Column(col_num,ws)
        cells.width = 5000

    for row in rows:
        row_num += 1
        a = [row[0],str(row[1]),row[2]]

        for col_num in range(len(a)):
            ws.col(0).width = 30 * 256
            ws.col(1).width = 100 * 256
            ws.col(2).width = 30 * 256
            ws.write(row_num, col_num, a[col_num], font_style)
        #
        #ws.write(row_num, , [[row.srvaddr],[row.namedisk], [str(row.freespace)],[str(row.allspace)],[row.date_up]], font_style)
    print('start adding new table', row_num)


    wb.save(f".\\reports\\Список документов без резолюции Артюхин Р.Е. {date.strftime('%Y-%m-%d')}.xls")