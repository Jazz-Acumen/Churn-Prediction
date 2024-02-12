

def create_engine():
    import numpy as np
    import pypyodbc
    import pandas as pd
    import warnings
    from datetime import datetime, timezone
    warnings.filterwarnings('ignore')

    conn = pypyodbc.connect("Driver={SQL Server};"
    "Server=10.50.15.254;"
    "Database=Mobilink;"
    "uid=HR_ANALYTICS;pwd=HR_ANALYTICS1")
    pypyodbc.lowercase = False

    import sqlalchemy as sa
    import urllib
    params = urllib.parse.quote_plus("Driver={SQL Server};"
                                    "Server=10.50.15.254;"
                                    "Database=Mobilink;"
                                    "uid=HR_ANALYTICS;pwd=HR_ANALYTICS1")
                                    # pypyodbc.lowercase = True

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

    return engine

  


  
def create_dataset():
    import numpy as np
    import pypyodbc
    import pandas as pd
    import warnings
    from datetime import datetime, timezone
    warnings.filterwarnings('ignore')

    engine = create_engine()
    sql = """

            select * FROM [Mobilink].[dbo].[NEW_PeopleData]
            where ID_Merged in (SELECT ID_Merged FROM [Mobilink].[dbo].[NEW_Turnover_ADS] where LAST_WORKING_DATE is not NULL)

            """

    data = pd.read_sql(sql,con=engine)
    emps = pd.DataFrame(data.ID_Merged.unique(), columns = ['emp'] )
    # latest divs and dept 
    data['TDATE'] = pd.to_datetime(data['TDATE'], utc=False)
    data['HIRE_DATE'] = pd.to_datetime(data['HIRE_DATE'], utc=False)
    data['DATE_OF_BIRTH'] = pd.to_datetime(data['DATE_OF_BIRTH'], utc=False)
    data['LAST_WORKING_DATE'] = pd.to_datetime(data['LAST_WORKING_DATE'], utc=False)
    data['emp'] = data['ID_Merged']
    df = data.sort_values(by='TDATE', ascending=False)
    latest_index = df.groupby('emp')['TDATE'].idxmax()
    latest_entries = df.loc[latest_index]
    latest_entries = latest_entries[['emp','GRADE_NAME', 'DIVISION', 'DEPARTMENT', 'SUB_DEPARTMENT', 'GENDER', 'MARITAL_STATUS', 'CITY', 'LOCATION', 'POSITION', 'HIRE_DATE', 'DATE_OF_BIRTH', 'LAST_WORKING_DATE']]
    emps = pd.merge(emps, latest_entries, on='emp')


    # calculating tenure and age
    current_date = datetime.now(timezone.utc)
    emps['tenure'] = (emps['LAST_WORKING_DATE'] - emps['HIRE_DATE']).dt.days / 365
    emps['age'] = (current_date - emps['DATE_OF_BIRTH']).dt.days / 365


    # adding count columns 

    """
    Number of counts we want: 
    1. Divisions
    2. Departments
    3. Sub departments
    4. Managers
    5. Position
    6. Grade

    """

    unique_values = df.groupby('emp').agg({
        'DIVISION': lambda x: x.nunique(dropna=True),
        'DEPARTMENT': lambda x: x.nunique(dropna=True),
        'SUB_DEPARTMENT': lambda x: x.nunique(dropna=True),
        'GRADE_NAME': lambda x: x.nunique(dropna=True),
        'POSITION':lambda x: x.nunique(dropna=True),
        'LM_NO': lambda x: x.nunique(dropna=True)
    }).reset_index().replace(0, 1)

    unique_values.columns = ['emp', 'DIVISION_count', 'DEPARTMENT_count', 'SUB_DEPARTMENT_count', 'GRADE_count', 'POSITION_count', 'LM_count']
    emps = pd.merge(emps, unique_values, on='emp')


    # Time in final grade

    df = df.sort_values(by=['emp', 'TDATE'], ascending=[True, False])
    df['Grade_Change'] = df['GRADE_NAME'] != df['GRADE_NAME'].shift(-1)
    df_grade_change = df.loc[df['Grade_Change']==True]
    emps_grade_change = pd.DataFrame(df_grade_change.groupby('emp')['TDATE'].max()).reset_index()
    emps_grade_change.columns = ['emp', 'last_grade_change']

    emps = pd.merge(emps, emps_grade_change, on='emp', how='left')
    emps['last_grade_change'] = pd.to_datetime(emps['last_grade_change']).dt.tz_localize('UTC')
    emps['time_last_grade'] = (emps['LAST_WORKING_DATE'] - emps['last_grade_change']).dt.days / 365


    # Time in final position

    df['POSITION_Change'] = df['POSITION'] != df['POSITION'].shift(-1)
    df_position_change = df.loc[df['POSITION_Change']==True]
    emps_position_change = pd.DataFrame(df_position_change.groupby('emp')['TDATE'].max()).reset_index()
    emps_position_change.columns = ['emp', 'last_POSITION_change']

    emps = pd.merge(emps, emps_position_change, on='emp', how='left')
    emps['last_POSITION_change'] = pd.to_datetime(emps['last_POSITION_change']).dt.tz_localize('UTC')
    emps['time_position_grade'] = (emps['LAST_WORKING_DATE'] - emps['last_POSITION_change']).dt.days / 365


    # Time with new manager


    df['Manager_Change'] = df['LM_NO'].dropna() != df['LM_NO'].dropna().shift(-1)
    df_manager_change = df.loc[df['Manager_Change']==True]
    emps_manager_change = pd.DataFrame(df_manager_change.groupby('emp')['TDATE'].max()).reset_index()
    emps_manager_change.columns = ['emp', 'last_manager_change']

    emps = pd.merge(emps, emps_manager_change, on='emp', how='left')
    emps['last_manager_change'] = pd.to_datetime(emps['last_manager_change']).dt.tz_localize('UTC')
    emps['time_manager_change'] = (emps['LAST_WORKING_DATE'] - emps['last_manager_change']).dt.days / 365



    # payroll features

    sql_payroll = """

    /****** Script for SelectTopNRows command from SSMS  ******/
    SELECT DISTINCT [emp #], [payroll month], hc.[GRADE_NAME],hc.POSITION ,[standard gross  ], Commissions
    FROM [Mobilink].[dbo].[Payroll_Long_Format] p
    left join (select x.*
    from (
        select ID_Merged, TDATE, GRADE_NAME, EMPLOYEE_NAME,GENDER,POSITION,[ROLE_JOB]
        ,[DIVISION]
        ,[DEPARTMENT]
        ,[SUB_DEPARTMENT]
        ,[SECTION]
        ,[SUB_SECTION]
        ,[LOCATION]
        ,[DATE_OF_BIRTH]
        ,[HIRE_DATE]
        ,[LAST_WORKING_DATE]
        ,[ASSIGNMENT_STATUS]
        ,[HR_STATUS]
        ,[CNIC]
        ,[NATIONALITY]
        ,[MARITAL_STATUS]
        ,[LM_NO]
        ,[LM_NAME]
        ,[HOD_NO]
        ,[HOD_NAME]
        ,[CITY]
        ,[RELIGION]
        ,[TRANSACTION_DATE]
        ,[LMGENDER],
        DIV_NAME,
            row_number() over (partition by ID_Merged order by TDATE desc) as _rn
        from [Mobilink].[dbo].[NEW_PeopleData]
    ) x
    where x._rn = 1
    
    
    ) hc on hc.ID_Merged = p.[emp #]

    """


    payroll = pd.read_sql(sql_payroll, con=engine)
    payroll['emp #'] = payroll['emp #'].astype(int).astype(str)
    payroll['payroll month'] = pd.to_datetime(payroll['payroll month'])
    ids = emps['emp'].unique()
    payroll_leavers = payroll.loc[payroll['emp #'].isin(ids)]
    p = pd.DataFrame(payroll_leavers.groupby(['emp #'])['payroll month'].max()).reset_index()
    p.columns = ['emp #', 'last_payroll']
    merged_df = pd.merge(payroll_leavers, p, on='emp #', how='inner')

    # Filter rows based on last_payroll_month
    filtered_payroll_df = merged_df[merged_df['payroll month'] == merged_df['last_payroll']]
    filtered_payroll_df.columns = ['emp', 'payroll month', 'GRADE_NAME','POSITION','standard gross', 'Commissions', 'last_payroll']
    filtered_payroll_df = filtered_payroll_df[['emp','payroll month', 'standard gross', 'Commissions']]

    # salary differences from grade 
    # need to get the average salary for the last payroll for each employee. 
    emps = pd.merge(emps, filtered_payroll_df, on='emp', how='left')
    level_avg_df = pd.DataFrame(payroll.groupby(['GRADE_NAME', 'payroll month'])['standard gross  '].agg(['median', 'std'])).reset_index()
    level_avg_df.columns = ['GRADE_NAME', 'payroll month','AverageSalary', 'std_salary']
    merged_df = pd.merge(emps, level_avg_df, on=['GRADE_NAME', 'payroll month'], how='left')

    # Calculate the standard deviation units from the level average
    merged_df['Salary_std'] = (merged_df['standard gross'] - merged_df['AverageSalary']) / np.std(merged_df['std_salary'])
    emps = merged_df.copy()
    last_working_dates_df = emps[['emp', 'LAST_WORKING_DATE']]
    changes_df = df[['ID_Merged', 'LOCATION', 'TDATE']]

    import pandas as pd
    from datetime import datetime, timedelta


    # Identify employees who left the company
    left_employees = last_working_dates_df['emp'].tolist()
    location_change_before_leaving = []

    for employee_id in left_employees:
        # Filter changes for the current employee
        employee_changes = changes_df[changes_df['ID_Merged'] == employee_id]
        # Check if any location changes occurred
        has_location_change = len(employee_changes['LOCATION'].unique()) > 1
        location_change_before_leaving.append({
            'emp': employee_id,
            'HadLocationChangeBeforeLeaving': has_location_change
        })
    result_df = pd.DataFrame(location_change_before_leaving)
    emps = pd.merge(emps, result_df, on='emp', how='left')
    emps.columns = ['emp', 'GRADE_NAME', 'DIVISION', 'DEPARTMENT', 'SUB_DEPARTMENT',
       'GENDER', 'MARITAL_STATUS', 'CITY', 'LOCATION', 'POSITION',
       'HIRE_DATE', 'DATE_OF_BIRTH', 'LAST_WORKING_DATE', 'tenure', 'age',
       'DIVISION_count', 'DEPARTMENT_count', 'SUB_DEPARTMENT_count',
       'GRADE_count', 'POSITION_count', 'LM_count', 'last_grade_change',
       'time_last_grade', 'last_POSITION_change', 'time_position_grade',
       'last_manager_change', 'time_manager_change', 'payroll month',
       'standard gross', 'Commissions', 'AverageSalary', 'std_salary',
       'Salary_std', 'HadLocationChangeBeforeLeaving']

    emps.loc[emps['LM_count']==1, 'last_manager_change'] = 0
    emps.loc[emps['LM_count']==1, 'time_manager_change'] = 0
    final_emps = emps.loc[~emps['payroll month'].isna()]
    date_columns = final_emps.select_dtypes(include=['datetime64[ns, UTC]']).columns
    date_columns = final_emps.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        final_emps[date_column] = final_emps[date_column].apply(lambda a: pd.to_datetime(a).date()) 

    temp = final_emps.copy()

    temp.to_excel('churn-final-dataset.xlsx',engine='xlsxwriter')



if __name__ == "__main__":
    create_dataset()