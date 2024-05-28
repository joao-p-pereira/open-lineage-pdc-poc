REM Define variables
set "ZIP_FILE=C:\path\to\your\file.zip"
set "TARGET_DIR=C:\path\to\target\directory"
set "SPOON_FILE=C:\Users\jpereira\hitachi\Pentaho\Software\data-integration\Spoon.bat"
set "PROGRAM_NAME=javaw"
Stop-Process -Name "javaw"

call "%SPOON_FILE%"
