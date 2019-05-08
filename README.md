1. Filter _init_mode_assignment_ to contain only target TANs in the quarter of interest. The number of entries in this file will correspond to the the number of unique TANs in the MCNF model. No duplicate TAN is allowed. For example, to extract UABU SRGBU CSPBU UCEBU INSBU DCSABU ECBU WNBU through FOC-FTX for FY19Q2 to an new file _init_mode_top_tans_, use the following command. This task can also be performed in excel.

   ```bash
   # Get header line
   cat initial_mode_assignment.csv | head -n 1 > init_mode_top_tans.csv
   
   # Perform filtering
   cat initial_mode_assignment.csv | grep "FY2019 Q2" | grep FOC | grep FTX | grep 'UABU\|SRGBU\|CSPBU\|UCEBU\|INSBU\|DCSABU\|ECBU\|WNBU' | wc -l
   ```

   

