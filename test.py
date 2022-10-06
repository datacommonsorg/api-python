import datacommons as dc

dc.set_api_key('AIzaSyCTI4Xz-UW_G2Q2RfknhcfdAnTHq5X5XuI')
data = dc.property_values(['geoId/06'], 'name')
print(data)