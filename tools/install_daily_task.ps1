# Optional system registration. Run explicitly after verifying the daily command.
$ErrorActionPreference = 'Stop'
$etlRoot = Split-Path -Parent $PSScriptRoot
if ((Get-TimeZone).BaseUtcOffset -ne [TimeSpan]::FromHours(8)) {
    throw 'Configure the Windows host for Philippine local time before registering the 02:00 task.'
}
$etlDailyScript = Join-Path $PSScriptRoot 'run_daily.ps1'
$etlAction = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument ('-NoProfile -File "' + $etlDailyScript + '"') -WorkingDirectory $etlRoot
$etlTrigger = New-ScheduledTaskTrigger -Daily -At '02:00'
$etlPrincipal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$etlSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable
Register-ScheduledTask -TaskName 'JowenTwoThree_ETL_Daily' -Action $etlAction -Trigger $etlTrigger -Principal $etlPrincipal -Settings $etlSettings -Description 'Bounded Manila-day extraction, approved transformation, and local warehouse load'
