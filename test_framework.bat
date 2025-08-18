@echo off
echo 🧪 Testing Framework CLI...
python framework-cli.py status
echo.
echo 📋 Testing pipeline creation...
python framework-cli.py create basic_etl teste_batch
echo.
echo ✅ Tests completed!
pause
