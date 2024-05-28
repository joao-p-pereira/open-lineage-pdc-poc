REM Define variables
set "ZIP_FILE=C:\Users\jpereira\hitachi\projects\open-lineage\assemblies\plugin\target\open-lineage-plugin-10.1.0.0-SNAPSHOT.zip"
set "TARGET_DIR=C:\Users\jpereira\hitachi\Pentaho\Software\data-integration\plugins\open-lineage-plugin"
set "PLUGINS_DIR=C:\Users\jpereira\hitachi\Pentaho\Software\data-integration\plugins"
set "SPOON_FILE=C:\Users\jpereira\hitachi\Pentaho\Software\data-integration\Spoon.bat"
set "PROGRAM_NAME=javaw.exe"

taskkill /F /IM "%PROGRAM_NAME%" 

timeout 1

rd /S /Q "%TARGET_DIR%"

powershell -Command "Expand-Archive -Force '%ZIP_FILE%' '%PLUGINS_DIR%'"

call "%SPOON_FILE%"
