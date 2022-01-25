from Deon_fish import *


mon = pd.read_csv(s3_client.get_object(Bucket=bucket_name, Key='python/fish-market-mon.csv')['Body'])
tues = pd.read_csv(s3_client.get_object(Bucket=bucket_name, Key='python/fish-market-tues.csv')['Body'])
wed = pd.read_csv(s3_client.get_object(Bucket=bucket_name, Key='python/fish-market.csv')["Body"])
full_df = pd.concat([mon, tues, wed], axis=0, ignore_index=True)



# print(expected)
def test_extract():
    assert extract().equals(full_df)


def test_transform():
    expected = full_df.groupby('Species').mean()
    actual = trans_to_csv()[1]
    for row in expected.head():
        for column in list(expected.index):
            assert expected[row][column] == (actual)[row][column]




def test_load():
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='Data26/fish/')
    names = []
    for obj in response['Contents']:
        names.append(obj['Key'])
    assert 'Data26/fish/Deon_Fish.csv' in names