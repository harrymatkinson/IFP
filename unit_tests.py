import unittest
from parse_files import format_stock

class TestFunctions(unittest.TestCase):
    def test_format_stock(self):
        # both of these inputs should return the same value = 100
        stock_int = 100
        stock_str = "100 abcdef"
        self.assertEqual(format_stock(stock_int), format_stock(stock_str))
        # passing None should raise a TypeError
        with self.assertRaises(TypeError):
            format_stock(None)

if __name__ == "__main__":
    unittest.main()
