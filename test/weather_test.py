from interview import weather
import io
import math
import csv

def test_data_processor_default():
    p = weather.TemperatureProcessor()
    assert p.start is None, 'date should default to None if no data been consumed'
    assert p.end is None, 'date should default to None if no data been consumed'
    assert math.isinf(p.high) and 1 > p.high, 'high should default to dummy negative infinity if no data been consumed'
    assert math.isinf(p.low) and 1 < p.low, 'high should default to dummy infinity if no data been consumed'

def test_data_processor():
    p = weather.TemperatureProcessor()
    temp = 0
    p.process(temp)
    assert p.start == temp, 'should update start from default'
    assert p.end == temp, 'should update end from default'
    assert p.high == temp, 'should update high from default'
    assert p.low == temp, 'should update low from default'

    new_high, new_low = 100, -100
    p.process(new_high)
    p.process(new_low)
    assert p.high == new_high, 'should update high'
    assert p.low == new_low, 'should update low'
    assert p.start == new_low, 'should update start every time'
    assert p.end == temp, 'should NOT update end'

def test_solution():
    fields = 'Station Name,Measurement Timestamp,Air Temperature'
    data1 = 'YellowStone,01/01/2022,3'
    data2 = 'YellowStone,01/01/2022,100'
    data3 = 'YellowStone,01/01/2022,50'
    data4 = 'Yosemite,02/02/2022,100'
    payload = '\n'.join([fields, data1, data2,data3, data4])
    reader = io.StringIO(payload)
    writer = io.StringIO()
    weather.process_csv(reader, writer)
    
    rows = writer.getvalue().split('\r\n')
    rows.remove("")
    assert len(rows) == 3
    assert rows[0] == ','.join(weather.TemperatureProcessor.output_fields)
    assert rows[1] == 'YellowStone,01/01/2022,3.0,100.0,50.0,3.0'
    assert rows[2] == 'Yosemite,02/02/2022,100.0,100.0,100.0,100.0'

