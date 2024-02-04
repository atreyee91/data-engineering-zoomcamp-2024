import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Preprocessing: rows with zero pax:", data['passenger_count'].isin([0]).sum())
    print("Preprocessing: rows with zero distance:", data['trip_distance'].isin([0]).sum())

    data=data.assign(lpep_pickup_date=pd.to_datetime(data['lpep_pickup_datetime']).dt.date)

    print(f"Columns of DataFrame not in snail case: {sum(data.columns.str.contains(' ')) | sum(data.columns.str.match(r'^[A-Z]'))}") 

    #Converting in Snail Case
    data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.lower()
                )     
    

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    data.rename(columns={'vendorid':'vendor_id'},inplace=True)

    return data

@test
def test_output(output, *args):
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 pax'
