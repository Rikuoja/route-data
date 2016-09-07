import operator

import fiona
import rtree
from shapely.geometry import shape, mapping, MultiLineString
from collections import OrderedDict

# first input must be a collection of linestrings
talvi = fiona.open("talvi2016-2017.lines.geojson")
# second input must be a collection of polygons (should? be cleaned with e.g. Grass in the ylre case)
# to be used preferentially
ylre = fiona.open("ylre_pyoravaylat.shp")
# third input must be a collection of polygons used as fallback if no match is found in the above network
# rtree index is required to calculate intersections in reasonable time
index = rtree.index.Index()

# fields to import from linestrings
import_linestring_fields = {'id': 'original_line_id'}
# fields to import from polygons
import_polygon_fields = {'osan_id': 'ylre_id',
                    'paatyyppi': 'type',
                    'paatyyppi_': 'type_id',
                    'alatyyppi': 'subtype',
                    'alatyyppi_': 'subtype_id',
                    'materiaali': 'material',
                    'materiaali_': 'material_id',
                    'rakenteell': 'maintainer',
                    'talvikunno': 'winter_maintainer',
                    'tkp_kiiree': 'winter_maintenance_class',
                    'yllapidon_': 'maintenance_class',
                    'yllapido_1': 'maintenance_reason',
                    'aluetieto': 'area_type',
                    'alueen_nim': 'area_name',
                    'paivitetty': 'last_modified_time'}

# linestring fields will override polygon fields
import_fields_as = import_polygon_fields.copy()
import_fields_as.update(import_linestring_fields)

# create output schema automatically from input schema
combined_properties = ylre.schema['properties'].copy()
combined_properties.update(talvi.schema['properties'])
import_fields_schema = {'geometry': talvi.schema['geometry'],
                        'properties': OrderedDict(
    [(import_fields_as[key], value) for key, value in combined_properties.items() if key in import_fields_as]
)}

# the final result
output2 = fiona.open("saved_routes.json",
                    'w',
                    driver=talvi.driver,
                    crs=talvi.crs,
                    schema=import_fields_schema)

# the unknown areas buffered
buffer_save = fiona.open("buffers.json",
                      'w',
                      driver=talvi.driver,
                      crs=talvi.crs,
                      schema={'properties': OrderedDict([('id', 'str')]), 'geometry': 'Polygon'})


ylre_list = list(ylre)
polygons = [shape(polygon['geometry']) for polygon in ylre_list]
polygon_metadata = [{import_fields_as[key]: value for key, value in polygon['properties'].items()
                     if key in import_fields_as}
                    for polygon in ylre_list]
print("Found area data")

for polygon_index, polygon in enumerate(polygons):
    index.insert(polygon_index, polygon.bounds)
print("Generated area index")

#total_area = MultiPolygon(polygons)

routes = [shape(linestring['geometry']) for linestring in talvi]
route_metadata = [{import_fields_as[key]: value for key, value in route['properties'].items()
                        if key in import_fields_as}
                  for route in talvi]

print("Found linestring data")

new_linestrings = []
new_linestrings_metadata = []

for route_index, linestring in enumerate(routes):
    candidate_indices = list(index.intersection(linestring.bounds))
    # first, split those parts of linestring that are within an area
    for polygon, metadata in [(polygons[x], polygon_metadata[x]) for x in candidate_indices]:
        route_in_polygon = linestring.intersection(polygon)
        if not route_in_polygon.is_empty:
            new_linestrings.append(route_in_polygon)
            # add the linestring metadata to polygon metadata
            metadata.update(route_metadata[route_index])
            # save the metadata
            new_linestrings_metadata.append(metadata)
            # remove the discovered section from any further matching
            linestring = linestring.difference(polygon)
            routes[route_index] = linestring

print("Matched routes to underlying areas")
    # convert the new linestrings array to shape
    #new_linestrings = MultiLineString(lines=new_linestrings)


    # will fail:
    # route_outside_areas = linestring.difference(total_area)

# the remaining pieces of the linestrings should be arrays, not multilinestrings:
print(routes)
routes = [list(route) for route in routes]

# TODO: route parts whose both ends are within 4 meters of the same bike lane polygon
line_end_buffers = [[line.boundary.buffer(4.0) for line in route] for route in routes]
print("Created line end buffers for finding matching bike lane at both ends")
for route_index, route_buffered in enumerate(line_end_buffers):
    for line_index, ends_buffered in enumerate(route_buffered):
        pass
        # TODO: area as the (unlikely) tiebreaker


# TODO: second iteration, more cutting according to any boundaries crossed, to get more granular match on non-bike lanes
# finally, map to the (car/path/whatever) lane with the most overlap by buffering the whole line:

# second, route parts that are within 4 m of an area, requires buffering
buffers = [[line.buffer(4.0) for line in route] for route in routes]
print("Created buffers for remaining parts of routes")

# save the buffer list flattened
for item in [item for sublist in buffers for item in sublist]:
    buffer_save.write({'geometry': mapping(item), 'properties': OrderedDict([('id', 'mock')])})
buffer_save.close()

for route_index, route_buffered in enumerate(buffers):
    for line_index, buffer in enumerate(route_buffered):
        nearby_polygons = []
        nearby_polygon_indices = []
        candidate_indices = list(index.intersection(buffer.bounds))
        for polygon_index, (polygon, metadata) in enumerate([(polygons[x], polygon_metadata[x]) for x in candidate_indices]):
            buffer_polygon_intersection = buffer.intersection(polygon)
            if not buffer_polygon_intersection.is_empty:
                nearby_polygons.append(buffer_polygon_intersection)
                nearby_polygon_indices.append(polygon_index)
                print('Found intersection between ' + str(buffer) + ' and ' + str(polygon))
        # quick and dirty approximation:
        # pick the one with the most overlap, do not cut the line further
        selected_polygon_index = nearby_polygon_indices[max(enumerate(map(lambda polygon: polygon.area, nearby_polygons)),
                                                            key=operator.itemgetter(1))[0]]

        new_linestrings.append(routes[route_index][line_index])
        # add the linestring metadata to polygon metadata
        metadata.update(route_metadata[route_index])
        # save the metadata
        new_linestrings_metadata.append(metadata)

print("Matched rest of the routes to nearby bike lanes or, failing that, regular lanes")

print("Stitching together pieces that belong together")
# TODO: look through the whole data and combine any linestrings with identical metadata!


# save the result in json
#output = fiona.open("leftover_routes.json",
#                    'w',
#                    driver=talvi.driver,
#                    crs=talvi.crs,
#                    schema=talvi.schema)
#for item in list(linestring):
#    output.write({'geometry': mapping(item), 'properties': OrderedDict([('id', 'mock')])})
#output.close()

for item, metadata in zip(new_linestrings, new_linestrings_metadata):
    output2.write({'geometry': mapping(item), 'properties': metadata})
output2.close()

