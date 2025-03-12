import os
import pandas as pd
from collections import defaultdict
from typing import Dict
from utils.general.io import save_data
from utils.general.processing import process_date_columns, rename_and_drop


class LTDProcessor:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.summary_data: Dict[str, defaultdict] = {}

        # Configuration
        self.chunk_size = 5000
        self.target_column = 'board'
        self.rename_map = {'calendar_date': 'DATE'}

        # Calculate drop columns dynamically based on kept columns
        self.keep_columns = ['DATE', self.target_column]
        self.drop_cols = None  # Will be set during first chunk processing

    def process_files(self):
        """Process all TSV files in input directory"""
        for filename in os.listdir(self.input_dir):
            if filename.endswith('.tsv'):
                self._process_single_file(filename)

    def _process_single_file(self, filename):
        """Process individual TSV file in chunks"""
        input_path = os.path.join(self.input_dir, filename)
        summary_dict = defaultdict(float)

        # Initialize chunk processing
        chunk_iterator = pd.read_csv(
            input_path,
            sep='\t',
            chunksize=self.chunk_size,
            low_memory=False
        )

        # Process each chunk
        for chunk in chunk_iterator:
            processed_chunk = self._process_chunk(chunk)
            self._update_summary(processed_chunk, summary_dict)

        # Save summary after processing all chunks
        self._save_summary(filename, summary_dict)

    def _process_chunk(self, chunk):
        """Process individual data chunk"""
        # Set drop columns on first chunk if not set
        if self.drop_cols is None:
            all_columns = set(chunk.columns)
            self.drop_cols = list(all_columns - set(self.keep_columns))

        # Drop unnecessary columns
        chunk = rename_and_drop(
            chunk,
            rename_map=self.rename_map,
            drop_cols=self.drop_cols
        )

        # Date processing
        chunk = process_date_columns(chunk)
        return chunk[self.keep_columns]  # Ensure column order

    def _update_summary(self, chunk, summary):
        """Update summary statistics with chunk data"""
        if self.target_column in chunk.columns:
            chunk_sum = chunk.groupby('DATE')[self.target_column].sum()
            for date, value in chunk_sum.items():
                summary[date] += value

    def _save_summary(self, filename, summary):
        """Save summary statistics for a file"""
        if not summary:
            return

        summary_df = pd.DataFrame({
            'DATE': summary.keys(),
            f'total_{self.target_column}': summary.values()
        }).sort_values('DATE')

        output_filename = filename.replace('_summary', '')
        save_data(
            summary_df,
            os.path.join(self.output_dir, output_filename)
        )
