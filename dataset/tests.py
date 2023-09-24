import unittest
import pandas as pd
import tempfile
import os


from dataset import get_symbols_data
class Test_get_symbols_data(unittest.TestCase):
    def setUp(self) -> None:
        self.ndt = get_symbols_data()
        return super().setUp()

    def test_get_symbols_data_returns(self):
        self.assertIsInstance(self.ndt, pd.DataFrame)

    def test_get_symbols_data_len_more_than_zero(self):
        self.assertTrue(len(self.ndt) > 0)


from dataset import check_dir_writable
class Test_check_dir_writable(unittest.TestCase):
    def test_check_dir_writable_tmp(self):
        self.assertTrue(check_dir_writable("/tmp"))

    def test_check_dir_writable_some_directory(self):
        self.assertFalse(check_dir_writable("/I/Hope?This?Does?Not?Exists"))


from dataset import clean_up_and_create_directory
class Test_clean_up_and_create_directory(unittest.TestCase):
    def test_clean_up_and_create_directory_creates(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            test_dir = tmpDir + "/test_dir"
            clean_up_and_create_directory(test_dir)
            self.assertTrue(os.path.isdir(test_dir))

    def test_clean_up_and_create_directory_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmpDir:
            test_loc = tmpDir + "/test_loc"
            with open(test_loc, "w") as f:
                f.write("Create a text file!")
            self.assertTrue(os.path.isfile(test_loc))
            clean_up_and_create_directory(test_loc)
            self.assertTrue(os.path.isdir(test_loc))


from dataset import fetch_symbol_data
class Test_fetch_symbol_data(unittest.TestCase):
    def test_fetch_symbol_data_AAPL(self):
        aapl = fetch_symbol_data("AAPL")
        self.assertIsInstance(aapl, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
