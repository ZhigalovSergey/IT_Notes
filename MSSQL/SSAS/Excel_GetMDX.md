### Извлечение запросов MDX из сводных таблиц в Excel файлах

Чтобы получить запрос MDX, который отправляет Excel кубу OLAP нужно использовать следующий макрос

```vbscript
Public Sub PivotDetails()
   Dim ws As Worksheet
   Dim qt As QueryTable
   Dim pt As PivotTable
   Dim pc As PivotCache
   Dim pf As PivotField

   For Each ws In ActiveWorkbook.Sheets

      For Each qt In ws.QueryTables
        ActiveCell.Value = "Sheet"
        ActiveCell.Offset(0, 1).Value = ws.Name

        ActiveCell.Offset(1, 0).Select
        ActiveCell.Value = "Data Source"
        ActiveCell.Offset(0, 1).Value = qt.Connection

        ActiveCell.Offset(1, 0).Select
        ActiveCell.Value = "Query"
        ActiveCell.Offset(0, 1).Value = qt.CommandText
      Next qt

      ActiveCell.Offset(2, 0).Select

      For Each pt In ws.PivotTables

        ActiveCell.Offset(1, 0).Select
        ActiveCell.Value = "Pivot Table"
        ActiveCell.Offset(0, 1).Value = pt.Name

        ActiveCell.Offset(1, 0).Select
        ActiveCell.Value = "Connection"
        ActiveCell.Offset(0, 1).Value = pt.PivotCache.Connection

        ActiveCell.Offset(1, 0).Select
        ActiveCell.Value = "SQL"
        ActiveCell.Offset(0, 1).Value = pt.PivotCache.CommandText

        ActiveCell.Offset(1, 0).Select
        ActiveCell.Value = "MDX"
        ActiveCell.Offset(0, 1).Value = pt.MDX

        For Each pf In pt.PageFields
            ActiveCell.Offset(1, 0).Select
            ActiveCell.Value = "Page"
            ActiveCell.Offset(0, 1).Value = pf.Name
            ActiveCell.Offset(0, 2).Value = pf.CurrentPageName
        Next pf

        For Each pf In pt.ColumnFields
            ActiveCell.Offset(1, 0).Select
            ActiveCell.Value = "Column"
            ActiveCell.Offset(0, 1).Value = pf.Name
        Next pf

        For Each pf In pt.RowFields
            ActiveCell.Offset(1, 0).Select
            ActiveCell.Value = "Row"
            ActiveCell.Offset(0, 1).Value = pf.Name
        Next pf

        For Each pf In pt.DataFields
            ActiveCell.Offset(1, 0).Select
            ActiveCell.Value = "Data"
        Next pf

      Next pt
   Next ws
End Sub
```



### Полезные ссылки:  

[Extract Datasource and Query from Excel Pivot](https://www.purplefrogsystems.com/blog/2008/01/extract-datasource-and-query-from-excel-pivot/)  