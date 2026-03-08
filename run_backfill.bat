@echo off
cd /d "C:\Users\thoma\OneDrive\Desktop\prizepicks_pipeline"
"C:\Users\thoma\AppData\Local\Programs\Python\Python313\python.exe" -m ml.backfill_outcomes >> logs\backfill.log 2>&1
