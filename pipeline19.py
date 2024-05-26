import luigi
import wget
import tarfile
import os
import pandas as pd
import io
import gzip
import shutil
import logging

# Класс для загрузки данных
class DownloadData(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')  # Параметр задачи, имя набора данных

    # Метод output указывает, какой файл будет создан в результате выполнения задачи
    def output(self):
        return luigi.LocalTarget(os.path.join('data', self.dataset_name + '_RAW.tar'))

    # Метод run описывает основные действия задачи
    def run(self):
        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file'
        if not os.path.exists('data'):
            os.makedirs('data')  # Создаем директорию, если она не существует
        try:
            logging.info(f"Downloading {self.dataset_name}...")
            wget.download(url, self.output().path)  # Скачиваем файл по URL
        except Exception as e:
            logging.error(f"Download failed: {e}")  # Логируем ошибку в случае неудачи
            raise  # Перебрасываем исключение дальше

# Класс для извлечения и обработки данных
class ExtractAndProcessData(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')  # Параметр задачи, имя набора данных

    # Метод requires указывает на зависимости задачи
    def requires(self):
        return DownloadData(dataset_name=self.dataset_name)

    # Метод run описывает основные действия задачи
    def run(self):
        tar_path = self.input().path
        extract_path = os.path.join('extracted', self.dataset_name)
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)  # Создаем директорию, если она не существует
        try:
            with tarfile.open(tar_path) as tar:
                logging.info(f"Extracting {self.dataset_name}...")
                tar.extractall(path=extract_path)  # Извлекаем файлы из архива

                # Получаем список всех файлов в архиве
                file_names = tar.getnames()
                
        except Exception as e:
            logging.error(f"Extraction failed: {e}")  # Логируем ошибку в случае неудачи
            raise
        finally:
            os.remove(tar_path)  # Удаляем архив после извлечения

        # Для каждого файла создаем отдельную папку и разархивируем его туда
        for item in file_names:
            item_path = os.path.join(extract_path, item)
            item_dir = os.path.join(extract_path, os.path.splitext(item)[0])
            if not os.path.exists(item_dir):
                os.makedirs(item_dir)  # Создаем папку для каждого файла
            if item.endswith('.gz'):
                self._process_gzip_file(item_path, item_dir)

    # Метод для обработки gzip файлов
    def _process_gzip_file(self, gz_path, extract_path):
        filename = os.path.splitext(os.path.basename(gz_path))[0]
        output_path = os.path.join(extract_path, filename)
        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    logging.info(f"Decompressing {gz_path}")
                    shutil.copyfileobj(f_in, f_out)  # Распаковываем файл
        except Exception as e:
            logging.error(f"Decompression failed: {e}")  # Логируем ошибку в случае неудачи
            raise
        finally:
            os.remove(gz_path)  # Удаляем gzip файл после распаковки

    # Метод output указывает, какой файл будет создан в результате выполнения задачи
    def output(self):
        return luigi.LocalTarget(os.path.join('extracted', self.dataset_name))

# Класс для сегментации таблиц
class SegmentTables(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')  # Параметр задачи, имя набора данных

    # Метод requires указывает на зависимости задачи
    def requires(self):
        return ExtractAndProcessData(dataset_name=self.dataset_name)

    # Метод run описывает основные действия задачи
    def run(self):
        extract_path = self.input().path
        for dir_name in os.listdir(extract_path):
            dir_path = os.path.join(extract_path, dir_name)
            if os.path.isdir(dir_path):
                for filename in os.listdir(dir_path):
                    file_path = os.path.join(dir_path, filename)
                    if not filename.endswith('.txt'):
                        continue
                    self._process_text_file(file_path, filename, dir_name)

    # Метод для обработки текстовых файлов
    def _process_text_file(self, file_path, filename, dir_name):
        dfs = {}  # Словарь для хранения DataFrame
        try:
            with open(file_path) as file:
                fio = io.StringIO()
                write_key = None
                for line in file:
                    if line.startswith('['):
                        if write_key:
                            fio.seek(0)
                            dfs[write_key] = pd.read_csv(fio, sep='\t', header='infer')
                            fio = io.StringIO()
                        write_key = line.strip('[]\n')
                        continue
                    fio.write(line)
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')

            # Создаем выходную директорию на основе имени файла, если она не существует
            output_dir = os.path.join('processed', self.dataset_name, dir_name)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Сохраняем данные в отдельные файлы (формат TSV)
            for key, df in dfs.items():
                output_path = os.path.join(output_dir, f'{filename.replace(".txt", f"_{key}.tsv")}')
                logging.info(f"Saving table {key} to {output_path}")
                df.to_csv(output_path, sep='\t', index=False)

                # Создаем и сохраняем сокращенную таблицу Probes (формат TSV)
                if key == "Probes":
                    probes_reduced_df = df.drop(
                        columns=["Definition", "Ontology_Component", "Ontology_Process", "Ontology_Function", "Synonyms", "Obsolete_Probe_Id", "Probe_Sequence"]
                    )
                    reduced_output_path = os.path.join(output_dir, 'Probes_reduced.tsv')
                    logging.info(f"Saving reduced Probes table to {reduced_output_path}")
                    probes_reduced_df.to_csv(reduced_output_path, sep='\t', index=False)

        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")  # Логируем ошибку в случае неудачи
            raise

    # Метод output указывает, какой файл будет создан в результате выполнения задачи
    def output(self):
        return luigi.LocalTarget(os.path.join('processed', self.dataset_name))

# Основной блок, выполняющий задачи
if __name__ == '__main__':
    luigi.build([SegmentTables(dataset_name='GSE68849')], local_scheduler=True)
