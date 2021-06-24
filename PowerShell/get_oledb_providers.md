# Получить список проайдеров OLEDB

Команда в PowerShell. **Execute from 32-bit (Windows PowerShell (86x)) and 64-bit (Windows PowerShell)  !!!**

```powershell
(New-Object system.data.oledb.oledbenumerator).GetElements() | select SOURCES_NAME, SOURCES_DESCRIPTION
```

Проверить битность PowerShell можно командой

```powershell
[IntPtr]::Size -eq 4 # True for 32 bit
[IntPtr]::Size -eq 8 # True for 64 bit
```



### Полезные ссылки:  

- [The "Microsoft.ACE.OLEDB.12.0" provider is not registered on the local computer](https://www.programmersought.com/article/47284659482/)  
- [Поставщик 'Microsoft.ACE.OLEDB.12.0' не зарегистрирован на локальном компьютере](https://coderoad.ru/Поставщик-Microsoft-ACE-OLEDB-12-0-не-зарегистрирован-на-локальном-компьютере)  