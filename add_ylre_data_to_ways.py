import operator

import fiona
import rtree
import os
from shapely.geometry import shape, mapping, LineString, MultiPolygon, Point
from shapely.geos import TopologicalError
from shapely.ops import linemerge
from collections import OrderedDict

# first input must be a collection of linestrings
ways = fiona.open("talvi2016-2017.lines.geojson")
# second input must be a collection of polygons
ylre = fiona.open("ylre_katu_ja_liikenne.shp")
# third (optional) input should be an additional collection of linestrings to be added to first input
# if objects with same ids are found, only the metadata is updated
# if we have new objects, they are added whole
additional_ways = fiona.open("talvi.osm.geojson")

# fields to import from linestrings (may override polygon fields, if not null or otherwise falsey)
import_linestring_fields = {'id': 'original_line_id',
                            'tags': 'osm_way_tags',
                            'tstamp': 'osm_tstamp',
                            'talviprojekti': 'winter_maintainer',
                            'tkp_kiiree': 'winter_maintenance_class'}
# fields to import from polygons (may contain duplicates, for fields with multiple source fields)
import_polygon_fields = {'osan_id': 'ylre_id',
                    'paatyyppi': 'type',
                    'paatyyppi_': 'type_id',
                    'alatyyppi': 'subtype',
                    'alatyyppi_': 'subtype_id',
                    'materiaali': 'material',
                    'materiaa_1': 'material_id',
                    'rakenteell': 'maintainer',
                    'talvikunno': 'winter_maintainer',
                    'tkp_kiiree': 'winter_maintenance_class',
                    'yllapidon_': 'maintenance_class',
                    'yllapido_1': 'maintenance_reason',
                    'yllapitolu': 'maintenance_class',
                    'yllapitolk': 'maintenance_reason',
                    'aluetieto': 'area_type',
                    'alueen_nim': 'area_name',
                    'kadun_nimi': 'area_name',
                    'paivitetty': 'last_modified_time'}
# metadata fields to reformat on import
reformat_fields = {'winter_maintenance_class': lambda value: 'winter_maintenance_project',
                   'winter_maintainer': lambda value: 'Rakennusvirasto Talvipyöräilyprojekti',
                   'original_line_id': lambda value: 'winter:' + str(value)}
# metadata fields to add on import
add_fields = {'id': lambda metadata: str(metadata.get('original_line_id')) + ':ylre:' + str(metadata.get('ylre_id'))}
# polygons to import from preferentially
preferred_polygon_filter = {'subtype_id': [6, 8, 9, 11, 471]}  # bike lane, combined bike&pedestrian lane/bridge
# polygons to ignore completely
ignored_polygon_filter = {'subtype_id': [6, 7, 10, 4],  # cubic stone, pedestrian zone, sidewalk, parking
                          'type_id': [23, 24, 25, 26, 27, 28, 29]}  # lawns, plantations, trees, forests,
                                                                # items, separators, walls

# linestring fields will override polygon fields
import_fields_as = import_polygon_fields.copy()
import_fields_as.update(import_linestring_fields)

# create output schema automatically from input schema
combined_properties = ylre.schema['properties'].copy()
combined_properties.update(talvi.schema['properties'])
import_fields_schema = {'geometry': talvi.schema['geometry'],
                        'properties': OrderedDict(
    [(value, combined_properties.get(key)) for key, value in import_fields_as.items()]
)}
# reformatted and added fields schema have to be added by hand using fiona.FIELD_TYPES_MAP
fiona_field_for_python_type = {value: key for key, value in fiona.FIELD_TYPES_MAP.items()}
reformatted_and_added_fields = reformat_fields.copy()
reformatted_and_added_fields.update(add_fields)
for field, function in reformatted_and_added_fields.items():
    try:
        import_fields_schema['properties'][field] = fiona_field_for_python_type[type(function({}))]
    except KeyError:
        raise TypeError("You are trying to create a new metadata field whose type does not correspond to any known Fiona field, "
                        "or the field creation function returned KeyError when facing incomplete metadata.")
# finally, use default 'str' schema for any metadata fields missing from input:
for field, schema in import_fields_schema['properties'].items():
    if not schema:
        import_fields_schema['properties'][field] = fiona_field_for_python_type[type('string')]

try:
    os.rename('saved_routes.json', 'saved_routes.json.old')
    os.rename('end_buffers.json', 'end_buffers.json.old')
    os.rename('buffers.json', 'buffers.json.old')
except FileNotFoundError:
    pass

# the final result
output2 = fiona.open("saved_routes.json",
                    'w',
                     driver=ways.driver,
                     crs=ways.crs,
                     schema=output_schema)

# the ends buffered
end_buffer_save = fiona.open("end_buffers.json",
                      'w',
                             driver=ways.driver,
                             crs=ways.crs,
                             schema={'properties': OrderedDict([('id', 'str')]), 'geometry': 'Polygon'})

# the unknown areas buffered
buffer_save = fiona.open("buffers.json",
                      'w',
                         driver=ways.driver,
                         crs=ways.crs,
                         schema={'properties': OrderedDict([('id', 'str')]), 'geometry': 'Polygon'})


def get_metadata_from_list(list):
    # do not overwrite existing values with empty values in case there are duplicate field names
    return [{import_fields_as[key]: value for key, value in item['properties'].items()
                     if key in import_fields_as and value}
                    for item in list]


def get_geometry_from_list(list):
    return [shape(item['geometry']) for item in list]

ylre_list = list(ylre)
polygons = get_geometry_from_list(ylre_list)

print("Found area data")

polygon_metadata = get_metadata_from_list(ylre_list)

# fill in the metadata to fit Fiona schema if values were empty
for item in polygon_metadata:
    for field in import_polygon_fields.values():
        if field not in item:
            item[field] = None
empty_polygon_metadata = {import_fields_as[key]: None for key in import_fields_as}

print("Found area metadata")

# rtree index is required to calculate intersections in reasonable time
index = rtree.index.Index()
for polygon_index, polygon in enumerate(polygons):
    index.insert(polygon_index, polygon.bounds)
print("Generated area index")

routes = get_geometry_from_list(ways)
print("Found route data")

route_metadata = get_metadata_from_list(ways)
print("Found route metadata")

additional_routes = get_geometry_from_list(additional_ways)
additional_route_metadata = get_metadata_from_list(additional_ways)
for additional_route_index, additional_metadata in enumerate(additional_route_metadata):
    for route_index, metadata in enumerate(route_metadata):
        if additional_metadata.get('original_line_id') == metadata.get('original_line_id'):
            # update the metadata for the route
            metadata.update(additional_metadata)
            break
    else:
        # the route was not found in original routes, we must append it
        routes.append(additional_routes[additional_route_index])
        route_metadata.append(additional_route_metadata[additional_route_index])

print("Found additional route data, merged additional data to original route data by id")

# fill in the metadata to fit Fiona schema if values were empty
for item in route_metadata:
    for field in import_linestring_fields.values():
        # we don't want existing (or added) empty values to override any data provided by polygon, so check that:
        if field in item and not item[field]:
            del item[field]
        if field not in item and field not in import_polygon_fields.values():
            item[field] = None

# then, onto the matching heuristic:

def add_to_remaining_routes(route_index, geometry):
    if isinstance(geometry, LineString):
        remaining_routes[route_index].append(geometry)
    elif isinstance(geometry, Point):
        # single points need not be considered
        return
    else:
        # we might have a geometrycollection, but must check for empties
        if not geometry.is_empty:
            for item in geometry:
                add_to_remaining_routes(route_index, item)


remaining_routes = [[] for route in routes]
new_linestrings = []
new_linestrings_metadata = []


def add_to_new_linestrings(geometry, metadata):
    if isinstance(geometry, LineString):
        # first, reformat metadata when a linestring is recognized
        for field, function in reformat_fields.items():
            metadata[field] = function(metadata[field])
        # then, add any additional metadata
        for field, function in add_fields.items():
            metadata[field] = function(metadata)
        new_linestrings.append(geometry)
        new_linestrings_metadata.append(metadata)
    elif isinstance(geometry, Point):
        # single points need not be considered
        return
    else:
        # we might have a geometrycollection, but must check for empties
        if not geometry.is_empty:
            for item in geometry:
                add_to_new_linestrings(item, metadata)

# 1) cut according to all boundaries crossed, direct match to any preferred polygons
for route_index, linestring in enumerate(routes):
    candidate_indices = list(index.intersection(linestring.bounds))
    for polygon, metadata in [(polygons[x], polygon_metadata[x]) for x in candidate_indices]:
        try:
            route_in_polygon = linestring.intersection(polygon)
            if not route_in_polygon.is_empty:
                # check if we matched preferred polygon
                for key, preferred_values in preferred_polygon_filter.items():
                    if metadata[key] in preferred_values:
                        # add the linestring metadata to polygon metadata
                        metadata.update(route_metadata[route_index])
                        add_to_new_linestrings(route_in_polygon, metadata)
                        break
                else:
                    # if there was no preferred match, leave unmatched for now
                    add_to_remaining_routes(route_index, route_in_polygon)
                # remove the discovered section from any further matching
                linestring = linestring.difference(polygon)
                routes[route_index] = linestring
        except TopologicalError:
            print('Ignoring invalid polygon')
    # finally, add the parts that didn't belong to a polygon
    add_to_remaining_routes(route_index, linestring)

print("Matched routes to underlying areas")
routes = list(remaining_routes)
remaining_routes = [[] for route in routes]


# 2) match nearby bike lanes
def add_the_polygon_with_most_overlap(route_index, line_index, polygon_indices, overlaps):
    selected_polygon_index = polygon_indices[max(enumerate(map(lambda polygon: polygon.area, overlaps)),
                                                        key=operator.itemgetter(1))[0]]
    #print(polygon_indices)
    #print(selected_polygon_index)
    #print(overlaps)
    metadata = polygon_metadata[selected_polygon_index]
    # add the linestring metadata to polygon metadata
    metadata.update(route_metadata[route_index])
    add_to_new_linestrings(routes[route_index][line_index], metadata)


def add_unmatched_line(route_index, line_index):
    metadata = empty_polygon_metadata
    # add the linestring metadata to polygon metadata
    metadata.update(route_metadata[route_index])
    add_to_new_linestrings(routes[route_index][line_index], metadata)

line_end_buffers = [[line.boundary.buffer(4.0) for line in route] for route in routes]
print("Created line end buffers for finding matching bike lane at both ends")

# save the end buffer list flattened
for item in [item for sublist in line_end_buffers for item in sublist]:
    end_buffer_save.write({'geometry': mapping(item), 'properties': OrderedDict([('id', 'mock')])})
end_buffer_save.close()

for route_index, route_buffered in enumerate(line_end_buffers):
    for line_index, ends_buffered in enumerate(route_buffered):
        nearby_bike_lanes = []
        nearby_bike_lane_indices = []
        candidate_indices = list(index.intersection(ends_buffered.bounds))
        # the index contains non-preferred polygons too
        for polygon_index, (polygon, metadata) in enumerate([(polygons[x], polygon_metadata[x]) for x in candidate_indices]):
            # check if the polygon is preferred
            for key, preferred_values in preferred_polygon_filter.items():
                if metadata[key] in preferred_values:
                    break
            else:
                # nah, we don't like the polygon enough
                continue
            try:
                buffer_polygon_intersection = ends_buffered.intersection(polygon)
                if not buffer_polygon_intersection.is_empty:
                    # for shorter pieces, the ends do not form a multipolygon so do not match them here
                    if not isinstance(ends_buffered, MultiPolygon):
                        continue
                    # do not add the intersection if the buffers are over 8 m apart and the polygon doesn't touch both
                    elif not isinstance(buffer_polygon_intersection, MultiPolygon):
                        continue
                    # print('Multipolygon ' + str(buffer_polygon_intersection) + ' found, matching line ' + str(line_index) + ' to polygon ' + str(polygon_index))
                    nearby_bike_lanes.append(buffer_polygon_intersection)
                    nearby_bike_lane_indices.append(candidate_indices[polygon_index])
            except TopologicalError:
                print('Ignoring invalid polygon')
        # pick the lane with the most overlap
        if nearby_bike_lanes:
            # print(nearby_bike_lane_indices)
            add_the_polygon_with_most_overlap(route_index, line_index, nearby_bike_lane_indices, nearby_bike_lanes)
        else:
            # if no lane was found straddling both ends, route will be added to remaining routes
            remaining_routes[route_index].append(routes[route_index][line_index])

print("Matched routes to bike lanes close by")
routes = list(remaining_routes)
remaining_routes = [[] for route in routes]


# 3) map to the (car/path/whatever) lane with the most overlap by buffering the whole line:
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
            # check if the polygon is ignored
            for key, ignored_values in ignored_polygon_filter.items():
                if metadata[key] in ignored_values:
                    break
            else:
                # we like the polygon too much to ignore it
                try:
                    buffer_polygon_intersection = buffer.intersection(polygon)
                    if not buffer_polygon_intersection.is_empty:
                        nearby_polygons.append(buffer_polygon_intersection)
                        nearby_polygon_indices.append(candidate_indices[polygon_index])
                        # print('Found intersection between ' + str(buffer) + ' and ' + str(polygon))
                except TopologicalError:
                    print('Ignoring invalid polygon')
        # quick and dirty approximation:
        # pick the one with the most overlap, do not cut the line further
        if nearby_polygons:
            add_the_polygon_with_most_overlap(route_index, line_index, nearby_polygon_indices, nearby_polygons)
        else:
            # if the linestring squarely falls in the midst of ignored polygons, it might show up as a gap
            add_unmatched_line(route_index, line_index)

print("Matched the rest of the routes to any lanes with the most overlap")

# 4) look through the whole data and combine any linestrings with identical metadata!
final_linestrings = []
final_metadata = []
for linestring_index, (linestring, metadata) in enumerate(zip(new_linestrings, new_linestrings_metadata)):
    # only go through the remaining indices, check the linestring isn't deleted yet
    if linestring:
        for index_to_compare, (string_to_compare, metadata_to_compare)\
                in list(enumerate(zip(new_linestrings, new_linestrings_metadata)))[linestring_index+1:]:
            # check the other string isn't deleted yet
            if string_to_compare and metadata == metadata_to_compare:
                if isinstance(linestring, LineString):
                    linestring = linemerge([linestring, string_to_compare])
                else:
                    # we already have a multilinestring
                    linestring_list = list(linestring)
                    linestring_list.append(string_to_compare)
                    linestring = linemerge(linestring_list)
                # don't consider the string ever again
                new_linestrings[index_to_compare] = None
        final_linestrings.append(linestring)
        final_metadata.append(metadata)

print("Stitched together pieces that belong together")

for item, metadata in zip(final_linestrings, final_metadata):
    output2.write({'geometry': mapping(item), 'properties': metadata})
output2.close()

