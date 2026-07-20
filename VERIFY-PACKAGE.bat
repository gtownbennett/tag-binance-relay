@echo off
echo Checking required TAG Relay files...
set missing=0

if not exist ".github\workflows\publish-image.yml" echo MISSING: .github\workflows\publish-image.yml & set missing=1
if not exist "Dockerfile" echo MISSING: Dockerfile & set missing=1
if not exist "requirements.txt" echo MISSING: requirements.txt & set missing=1
if not exist "app\main.py" echo MISSING: app\main.py & set missing=1
if not exist "app\ledger.py" echo MISSING: app\ledger.py & set missing=1
if not exist "app\__init__.py" echo MISSING: app\__init__.py & set missing=1

if "%missing%"=="0" (
  echo.
  echo SUCCESS: All required files are present.
) else (
  echo.
  echo ERROR: One or more required files are missing.
)

echo.
pause
