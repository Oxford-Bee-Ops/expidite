import shutil

import pandas as pd
from sensor_core import configuration as root_cfg
from sensor_core import file_naming
from sensor_core.cloud_connector import CloudConnector

logger = root_cfg.setup_logger("sensor_core")

class CloudUtilities:

    @staticmethod
    def download_journal_set(container_name: str, type_id: str) -> pd.DataFrame:
        """
        Downloads CSV files from a cloud storage container with the specified prefix and 
        combines them into a single DataFrame.

        Args:
            cloud_connector (CloudConnector): An instance of CloudConnector to interact with cloud storage.
            container_name (str): The name of the cloud storage container.
            prefix (str): The prefix to filter files in the container.
            download_path (str): The local directory to save the downloaded files.

        Returns:
            pd.DataFrame: A DataFrame containing the combined data from all downloaded CSV files.
        """
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        tmp_dir = file_naming.get_temporary_dir()
        files = cc.list_cloud_files(container_name, prefix=f"V3_{type_id}", suffix=".csv")
        cc.download_container(src_container=container_name, 
                              dst_dir=tmp_dir,
                              files=files)
        df_list = []
        for file in tmp_dir.glob("*.csv"):
            df = pd.read_csv(file)
            if not df.empty:
                df_list.append(df)
        if df_list:
            combined_dataframe = pd.concat(df_list, ignore_index=True)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return combined_dataframe
        else:
            logger.warning(f"No CSV files found in {tmp_dir}.")
            return pd.DataFrame()