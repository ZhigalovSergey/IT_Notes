```powershell
@Echo Off

Set FDir=%cd%
Set Maska="*.dll"

FOR /R %FDir% %%i IN (%Maska%) DO Call :Obrabotka "%%i" "%%~ni"
GoTo :EOF 

:Obrabotka
rem For install
Echo %1 >> GAC_Reg.log
gacutil /i %1 >> GAC_Reg.log

rem For uninstall
rem Echo %2 >> GAC_UnReg.txt
rem gacutil /u %2

rem gacutil /il C:\Users\zhigalov\Desktop\GOODS\TASKS\3905\lib\GACInstall.txt
rem gacutil /l Google.Cloud.BigQuery.V2
rem gacutil /i 
rem gacutil /u Google.Apis
rem gacutil /ul GAC_UnReg.txt
```

