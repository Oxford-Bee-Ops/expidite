# Usage: AHT20 crc8 checker.
# A total of 6 * 8 bits data need to check. G(x) = x8 + x5 + x4 + 1 -> 0x131(0x31), Initial value = 0xFF.
# No XOROUT.
# Author: XU Zifeng.
# Email: zifeng.xu@foxmail.com
N_DATA = 6
# 1 * 8 bits CRC
N_CRC = 1
# Initial value. Equal to bit negation the first data (status of AHT20)
INIT = 0xFF
# Useful value to help calculate
LAST_8_bit = 0xFF


# Devide number retrieve from CRC-8 MAXIM G(x) = x8 + x5 + x4 + 1
CRC_DEVIDE_NUMBER = 0x131

# Data and CRC taken from AHT20, use this for testing?
TEST_DATA = [
    [28, 184, 245, 165, 156, 208, 163],
    [28, 185, 16, 149, 156, 83, 112],
    [28, 184, 249, 85, 156, 114, 213],
    [28, 185, 9, 53, 156, 54, 45],
    [28, 185, 70, 117, 156, 189, 33],
    [28, 185, 64, 165, 156, 61, 209],
]


def mod2_division_8bits(a, b, number_of_bytes, init_value):
    """Calculate mod2 division in 8 bits. a mod b. init_value is for crc8 init value."""
    head_of_a = 0x80
    # Processing a
    a = a << 8
    # Preprocessing head_of_a
    for _ in range(number_of_bytes):
        head_of_a = head_of_a << 8
        b = b << 8
        init_value = init_value << 8
    a = a ^ init_value
    while head_of_a > 0x80:
        # Find a 1
        if head_of_a & a:
            head_of_a = head_of_a >> 1
            b = b >> 1
            a = a ^ b
        else:
            head_of_a = head_of_a >> 1
            b = b >> 1
        # This will show calculate the remainder
        # print("a:{0}\thead of a:{1}\tb:{2}".format(
        #     bin(a), bin(head_of_a), bin(b)))
    return a


def AHT20_crc8_calculate(all_data_int):
    init_value = INIT
    # Preprocess all the data and CRCCode from AHT20
    data_from_AHT20 = 0x00
    # Preprocessing the first data (status)
    for i_data in range(len(all_data_int)):
        data_from_AHT20 = (data_from_AHT20 << 8) | all_data_int[i_data]
    return mod2_division_8bits(data_from_AHT20, CRC_DEVIDE_NUMBER, len(all_data_int), init_value)


def AHT20_crc8_check(all_data_int):
    """The input data should be:
    Status Humidity0 Humidity1 Humidity2|Temperature0 Temperature1 Temperature2 CRCCode.
    In python's int64.
    """
    mod_value = AHT20_crc8_calculate(all_data_int[:-1])
    return mod_value == all_data_int[-1]


def CRC8_check(all_data_int, init_value=0x00):
    divider = 0x107
    DATA_FOR_CHECK = all_data_int[0]
    for data in all_data_int[1:-1]:
        DATA_FOR_CHECK = (DATA_FOR_CHECK << 8) | data
    remainder = mod2_division_8bits(DATA_FOR_CHECK, divider, len(all_data_int) - 1, init_value)
    return remainder == all_data_int[-1]


if __name__ == "__main__":
    print(CRC8_check([0x66, 0x44, 0x33, 0x22, 0x24], 0))
    for data in TEST_DATA:
        print(AHT20_crc8_check(data))
