#!/usr/bin/env python3

from async_requests import req0
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from mpl_toolkits.mplot3d import Axes3D
import asyncio
import numpy as np
import pandas as pd
import glob
import os


jsonhead = {"Content-Type": "application/json"}

async def get_vol_std(asset):
        vola = await req0(f'https://www.deribit.com/api/v2/public/get_historical_volatility?currency={asset}',
                          jsonhead).reqjson0()
        df = pd.DataFrame(vola, columns=['timestamp', 'DVOL'])
        # Convert timestamp to datetime                                                                              
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        # Set timestamp as index                                                                                     
        df.set_index('timestamp', inplace=True)
        #filter df to settlement time volatilities                                                                   
        daily_df = df.at_time('08:00')

        daily_df['delta_iv'] = daily_df['DVOL'].diff()

        # Calculate mean and standard deviation of daily changes
        mean_delta_iv = daily_df['delta_iv'].mean()
        std_delta_iv = daily_df['delta_iv'].std()
        
        # Threshold for 2 standard deviations above the mean
        threshold = mean_delta_iv + 2 * std_delta_iv

        return threshold


async def main():

    asset = 'BTC' #input("Enter the asset to animate options OI history [BTC,ETH]: ")
    mat0 = '31JAN25' #input("Enter the 1st maturity to animate: ")
    mat1 = '28MAR25' #input("Enter the 2nd maturity to animate: ")
    mat2 = '27JUN25' #input("Enter the 3rd maturity to animate: ")
    mat3 = '26SEP25' #input("Enter the 3rd maturity to animate: ")
    
    # Specify the path to the folder containing hdf5 files
    path0 = f'/home/bachelier/code/python/finance/db_optionbot/vol_surf/JAN/'
    path1 = f'/home/bachelier/code/python/finance/db_optionbot/vol_surf/MAR/'
    path2 = f'/home/bachelier/code/python/finance/db_optionbot/vol_surf/JUN/'
    path3 = f'/home/bachelier/code/python/finance/db_optionbot/vol_surf/SEP/'

    # Get a list of all hdf5 files in the folder
    hdf5_files0 = glob.glob(os.path.join(path0, f'{asset}*.h5'))
    hdf5_files1 = glob.glob(os.path.join(path1, f'{asset}*.h5'))
    hdf5_files2 = glob.glob(os.path.join(path2, f'{asset}*.h5'))
    hdf5_files3 = glob.glob(os.path.join(path3, f'{asset}*.h5'))
    
    # Create a dictionary to store DataFrames
    dataframes0 = {}
    dataframes1 = {}
    dataframes2 = {}
    dataframes3 = {}
    
    # Read each CSV file into a separate DataFrame
    for file0,file1,file2,file3 in zip(hdf5_files0,hdf5_files1,hdf5_files2,hdf5_files3):
        # Extract the filename without extension
        filename0 = os.path.splitext(os.path.basename(file0))[0]
        filename1 = os.path.splitext(os.path.basename(file1))[0]
        filename2 = os.path.splitext(os.path.basename(file2))[0]
        filename3 = os.path.splitext(os.path.basename(file3))[0]
        
        # Read the hdf5 file into a DataFrame
        dataframes0[filename0] = pd.read_hdf(file0, key=filename0)
        dataframes1[filename1] = pd.read_hdf(file1, key=filename1)
        dataframes2[filename2] = pd.read_hdf(file2, key=filename2)
        dataframes3[filename3] = pd.read_hdf(file3, key=filename3)


    # Create a list of datetime objects
    start_date = datetime(2024, 12, 31, 16, 25)  # Starting from the first datetime we collected data
    num_datetimes = len(hdf5_files0)  
    date_list = [start_date + timedelta(minutes=5*i) for i in range(num_datetimes)]
    dt_list = [dt.strftime("%Y-%m-%d %H:%M") for dt in date_list]


    # Create filenames based on the datetime objects
    hdf5_names0 = [f"{asset}-{mat0}_IV_{dt}" for dt in dt_list]
    hdf5_names1 = [f"{asset}-{mat1}_IV_{dt}" for dt in dt_list]
    hdf5_names2 = [f"{asset}-{mat2}_IV_{dt}" for dt in dt_list]    
    hdf5_names3 = [f"{asset}-{mat3}_IV_{dt}" for dt in dt_list]


    df_list0 = [dataframes0[each] for each in hdf5_names0]
    df_list1 = [dataframes1[each] for each in hdf5_names1]
    df_list2 = [dataframes2[each] for each in hdf5_names2]
    df_list3 = [dataframes3[each] for each in hdf5_names3]


    fig, ax = plt.subplots(subplot_kw={'projection': '3d'})
    #fig, ax = plt.subplots(projection='3d')
    #fig = plt.figure(figsize=(12, 6))
    #ax1 = fig.plot(121, projection='3d')
    #ax2 = fig.add_subplot(122, projection='3d')

    threshold = await get_vol_std('BTC')
    
    def animate(i):
        #ax1.clear()
        #ax2.clear()

        ax.clear()
        
        df0 = df_list0[i]
        df1 = df_list1[i]
        df2 = df_list2[i]
        df3 = df_list3[i]

        df0['cpIV'] = np.where(df0.index < 6, df0['put_IV'], df0['call_IV'])
        df1['cpIV'] = np.where(df1.index < 6, df1['put_IV'], df1['call_IV'])
        df2['cpIV'] = np.where(df2.index < 6, df2['put_IV'], df2['call_IV'])
        df3['cpIV'] = np.where(df3.index < 6, df3['put_IV'], df3['call_IV'])
        
        # Combine dataframes
        dfs = [df0, df1, df2, df3]
        maturities = [mat0, mat1, mat2, mat3]

        
        # Create meshgrid
        X = np.arange(len(df0['strikes']))
        Y = np.arange(len(maturities))
        X, Y = np.meshgrid(X, Y)

        Z_cp = np.array([df['cpIV'].values for df in dfs])
        
        
        # Plot Call OI
        surf = ax.plot_surface(X, Y, Z_cp, cmap='viridis')
        ax.set_title(f'OTM Call/Put Open Interest {hdf5_names0[i][-16:]}')
        ax.set_xlabel('Strikes')
        ax.set_ylabel('Maturities')
        ax.set_zlabel('IV')
        
        ax.set_xticks(np.arange(len(df0['strikes'])))
        ax.set_xticklabels(df0['strikes'])
        ax.set_yticks(np.arange(len(maturities)))
        ax.set_yticklabels(maturities)

        

        if i > 0:
            prev_df0 = df_list0[i-1]
            prev_df1 = df_list1[i-1]
            prev_df2 = df_list2[i-1]
            prev_df3 = df_list3[i-1]

            prev_dfs = [prev_df0,prev_df1,prev_df2,prev_df3]
        
            cp_pct_change = np.array( [ (( df['cpIV'] - prev_df['cpIV'] ) / prev_df['cpIV']) for df,prev_df in zip(dfs,prev_dfs)] )

            #print(cp_pct_change)
            cp_mask = np.abs(cp_pct_change) > threshold

        
            ax.scatter(X[cp_mask], Y[cp_mask], Z_cp[cp_mask], color='red', s=100)
        


        
        
        return surf

    ani = FuncAnimation(fig, animate, frames=len(df_list0), interval=500, blit=False)
    plt.tight_layout()
    #plt.show()
    ani.save(f'{asset}_IV_surface_2std.mp4', writer='ffmpeg')


asyncio.run(main())
