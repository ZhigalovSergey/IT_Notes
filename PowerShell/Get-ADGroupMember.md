# Получить список членов группы AD

Команда в PowerShell

```powershell
Get-ADGroupMember finance_writter | select name
```

Используя фильтр

```powershell
Get-ADGroupMember mdwh_readonly | where name -Like "*Ант*"| select name
```

