import operator

import fiona
import rtree
import os
from shapely.geometry import shape, mapping, LineString, MultiPolygon, Point
from shapely.geos import TopologicalError
from shapely.ops import linemerge
from collections import OrderedDict, abc

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
                   'original_line_id': lambda value: 'osm:' + str(value)}
# metadata fields to add on import
add_fields = {'id': lambda metadata: str(metadata.get('original_line_id')) +
                                     (':ylre:' if metadata.get('ylre_id') else '') +
                                     str(metadata.get('ylre_id'))}
# polygons to import from preferentially
preferred_polygon_filter = {'subtype_id': [6, 8, 9, 11, 471]}  # bike lane, combined bike&pedestrian lane/bridge
# polygons to ignore completely
ignored_polygon_filter = {'subtype_id': [6, 7, 10, 4],  # cubic stone, pedestrian zone, sidewalk, parking
                          'type_id': [23, 24, 25, 26, 27, 28, 29]}  # lawns, plantations, trees, forests,
                                                                # items, separators, walls

# linestring fields will override polygon fields
import_fields_as = import_polygon_fields.copy()
import_fields_as.update(import_linestring_fields)
empty_polygon_metadata = {import_fields_as[key]: None for key in import_fields_as}


def merge_metadata(line, polygon):
    metadata = polygon['metadata']
    # linestring metadata will override polygon metadata only if provided
    for key, value in line['metadata'].items():
        if value:
            metadata[key] = value
    # fill in the metadata to fit Fiona schema if values were empty
    for field in import_fields_as.values():
        if field not in metadata:
            metadata[field] = None
    # format for output
    for field, function in reformat_fields.items():
        metadata[field] = function(metadata[field])
    # then, add any additional metadata
    for field, function in add_fields.items():
        metadata[field] = function(metadata)
    return metadata


def get_output_metadata_schema():
    # create output schema automatically from input schema
    combined_properties = ylre.schema['properties'].copy()
    combined_properties.update(ways.schema['properties'])
    import_fields_schema = {'geometry': ways.schema['geometry'],
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
    return import_fields_schema

#try:
os.rename('saved_routes.json', 'saved_routes.json.old')
os.rename('pieces.json', 'pieces.json.old')
os.rename('end_buffers.json', 'end_buffers.json.old')
os.rename('buffers.json', 'buffers.json.old')
#except FileNotFoundError:
#    pass

# the final result
output2 = fiona.open("saved_routes.json",
                    'w',
                     driver=ways.driver,
                     crs=ways.crs,
                     schema=get_output_metadata_schema())

# the cut pieces
pieces_save = fiona.open("pieces.json",
                      'w',
                             driver=ways.driver,
                             crs=ways.crs,
                             schema={'properties': OrderedDict([('id', 'str')]), 'geometry': 'LineString'})

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


def get_geometry_and_metadata_from_list(list):
    # empty coordinate arrays will crash shapely, we must delete any empty geometries anon
    list = [item for item in list if item['geometry']['coordinates']]
    # do not overwrite existing values with empty values in case there are duplicate field names
    return [{'geometry': shape(item['geometry']),
             'metadata': {import_fields_as[key]: value for key, value in item['properties'].items()
                          if key in import_fields_as and value}}
            for item in list]

ylre_list = list(ylre)
polygons = get_geometry_and_metadata_from_list(ylre_list)

print("Found area data")

# rtree index is required to calculate intersections in reasonable time
index = rtree.index.Index()
for polygon_index, polygon in enumerate(polygons):
    index.insert(polygon_index, polygon['geometry'].bounds)
print("Generated area index")

ways_list = list(ways)
original_routes = get_geometry_and_metadata_from_list(ways_list)
print("Found route data")

additional_ways_list = list(additional_ways)
additional_routes = get_geometry_and_metadata_from_list(additional_ways_list)
for additional_route in additional_routes:
    for original_route in original_routes:
        if additional_route['metadata'].get('original_line_id') == original_route['metadata'].get('original_line_id'):
            # update the metadata for the route
            original_route['metadata'].update(additional_route['metadata'])
            break
    else:
        # the route was not found in original routes, we must append it
        original_routes.append(additional_route)
print("Found additional route data, merged additional data to original route data by id")

# then, onto the matching heuristic:


class RouteList(abc.MutableSequence):
    # all created routes and their metadata must be validated and multigeometries flattened
    # this prevents crashes analysing the geometry and writing to the desired schema
    # also, the list maintains an index of original routes to enable merging them after processing

    def __init__(self, iterable=()):
        self._list = list(iterable)
        self.original_routes = {}

    def __len__(self):
        return len(self._list)

    # no need for assignment or deletion
    def __setitem__(self, index, route):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def insert(self, index, route):
        if self._validate(route):
            if isinstance(route['geometry'], LineString):
                if route['geometry'].length < 0.001:
                # discard any linestrings below tolerance, such parts are generated by rounding errors
                    return
                self._list.insert(index, route)
                self._insert_to_index(index, route)
            else:
                # we have a geometrycollection, so we split it
                for item in route['geometry']:
                    self.insert(index, {'geometry': item, 'metadata': route['metadata']})

    def __getitem__(self, index):
        return self._list[index]

    def __contains__(self, x):
        return x in self._list

    def _insert_to_index(self, index, route):
        original_line_id = route['metadata']['original_line_id']
        if not self.original_routes.get(original_line_id):
            self.original_routes[original_line_id] = []
        self.original_routes[original_line_id].insert(index, route)

    def _validate(self, route):
        geometry = route['geometry']
        metadata = route['metadata']
        # fill in missing metadata to fit Fiona schema if values were empty
        for field in import_fields_as.values():
            if field not in metadata:
                metadata[field] = None
        # points, cyclical strings and empty geometries will crash the analysis
        return not geometry.is_empty and not geometry.is_ring and not isinstance(geometry, Point)


def pick_polygon_with_largest_area(line, polygons):
    selected_polygon = polygons[max(enumerate(map(lambda polygon: polygon['geometry'].area, polygons)),
                                                        key=operator.itemgetter(1))[0]]
    return merge_metadata(line, selected_polygon)

remaining_routes = RouteList()
new_routes = RouteList()

# 1) cut according to all boundaries crossed, direct match to any preferred polygons
for route_index, linestring in enumerate(original_routes):
    candidate_indices = list(index.intersection(linestring['geometry'].bounds))
    for polygon in [polygons[x] for x in candidate_indices]:
        try:
            route_in_polygon = linestring['geometry'].intersection(polygon['geometry'])
            if not route_in_polygon.is_empty:
                # check if we matched preferred polygon
                for key, preferred_values in preferred_polygon_filter.items():
                    if polygon['metadata'][key] in preferred_values:
                        new_routes.append({'geometry': route_in_polygon, 'metadata': merge_metadata(linestring, polygon)})
                        break
                else:
                    # if there was no preferred match, leave unmatched for now
                    remaining_routes.append({'geometry': route_in_polygon, 'metadata': linestring['metadata']})
                # remove the discovered section from any further matching
                linestring.update({'geometry': linestring['geometry'].difference(polygon['geometry'])})
                original_routes[route_index] = linestring
        except TopologicalError:
            print('Ignoring invalid polygon')
    # finally, add the parts outside polygons
    remaining_routes.append(linestring)
print("Matched routes to underlying areas")

for item in remaining_routes:
    pieces_save.write({'geometry': mapping(item['geometry']), 'properties': ({'id': 'remaining'})})
for item in new_routes:
    pieces_save.write({'geometry': mapping(item['geometry']), 'properties': ({'id': 'new'})})
pieces_save.close()

# 2) match nearby bike lanes
for line in remaining_routes:
    line['end_buffers'] = line['geometry'].boundary.buffer(4.0)
    end_buffer_save.write({'geometry': mapping(line['end_buffers']),
                           'properties': OrderedDict([('id', line['metadata']['original_line_id'])])})
end_buffer_save.close()
print("Created line end buffers for finding matching bike lane at both ends")
still_remaining_routes = RouteList()

for line in remaining_routes:
    if isinstance(line['end_buffers'], MultiPolygon):
        nearby_bike_lanes = []
        candidate_indices = list(index.intersection(line['end_buffers'].bounds))
        # the index contains non-preferred polygons too
        for polygon in [polygons[x] for x in candidate_indices]:
            # check if the polygon is preferred
            for key, preferred_values in preferred_polygon_filter.items():
                if polygon['metadata'][key] in preferred_values:
                    break
            else:
                # nah, we don't like the polygon enough
                continue
            try:
                # print('comparing buffer ' + str(line['end_buffers']) + ' to polygon ' + str(polygon['geometry']))
                buffer_polygon_intersection = line['end_buffers'].intersection(polygon['geometry'])
                if not buffer_polygon_intersection.is_empty:
                    # discard the intersection if the buffers are over 8 m apart and the polygon doesn't touch both
                    if not isinstance(buffer_polygon_intersection, MultiPolygon):
                        continue
                    # print('Multipolygon ' + str(buffer_polygon_intersection) + ' found, matching line ' + str(line) + ' to polygon ' + str(polygon))
                    nearby_bike_lanes.append({'geometry': buffer_polygon_intersection,
                                             'metadata': polygon['metadata']})
            except TopologicalError:
                print('Ignoring invalid polygon')
        # pick the lane with the most overlap
        if nearby_bike_lanes:
            new_routes.append({'geometry': line['geometry'],
                               'metadata': pick_polygon_with_largest_area(line, nearby_bike_lanes)})
        else:
            # if no lane was found straddling both ends, route will remain
            still_remaining_routes.append(line)
    # for shorter pieces, the ends do not form a multipolygon so do not match them here
    else:
        still_remaining_routes.append(line)


print("Matched routes to bike lanes close by")
remaining_routes = still_remaining_routes

# 3) map to the (car/path/whatever) lane with the most overlap by buffering the whole line:
for line in remaining_routes:
    line['buffer'] = line['geometry'].buffer(4.0)
    buffer_save.write({'geometry': mapping(line['buffer']),
                       'properties': OrderedDict([('id', line['metadata']['original_line_id'])])})
buffer_save.close()
print("Created buffers for remaining parts of routes")

for line in remaining_routes:
    nearby_polygons = []
    candidate_indices = list(index.intersection(line['buffer'].bounds))
    for polygon in [polygons[x] for x in candidate_indices]:
        # check if the polygon is ignored
        for key, ignored_values in ignored_polygon_filter.items():
            if polygon['metadata'][key] in ignored_values:
                break
        else:
            # we like the polygon too much to ignore it
            try:
                buffer_polygon_intersection = line['buffer'].intersection(polygon['geometry'])
                if not buffer_polygon_intersection.is_empty:
                    # print('Found intersection between ' + str(buffer) + ' and ' + str(polygon))
                    nearby_polygons.append({'geometry': buffer_polygon_intersection,
                                            'metadata': polygon['metadata']})
            except TopologicalError:
                print('Ignoring invalid polygon')
    # quick and dirty approximation:
    # pick the one with the most overlap, do not cut the line further
    if nearby_polygons:
        new_routes.append({'geometry': line['geometry'],
                           'metadata': pick_polygon_with_largest_area(line, nearby_polygons)})
    else:
        # if the linestring squarely falls in the midst of ignored polygons, it might show up as a gap
        polygon = {'metadata': empty_polygon_metadata}
        new_routes.append({'geometry': line['geometry'],
                           'metadata': merge_metadata(line, polygon)})
print("Matched the rest of the routes to any lanes with the most overlap")

# 4) look through the whole data and combine any linestrings with identical metadata!
final_routes = []
# we must speed up matching by doing it separately on each original route, otherwise we will scale O(n^2)
for original_route in new_routes.original_routes.values():
    for route_index, route in enumerate(original_route):
        # only go through the remaining indices, check the route isn't deleted yet
        if route:
            for index_to_compare, route_to_compare in\
                    enumerate([another_route for another_route in original_route[route_index+1:]]):
                if route_to_compare and route['metadata'] == route_to_compare['metadata']:
                    if isinstance(route['geometry'], LineString):
                        route['geometry'] = linemerge([route['geometry'], route_to_compare['geometry']])
                    else:
                        # we already have a multilinestring
                        linestring_list = list(route['geometry'])
                        linestring_list.append(route_to_compare['geometry'])
                        route['geometry'] = linemerge(linestring_list)
                    # don't consider the string ever again
                    original_route[index_to_compare] = None
            final_routes.append(route)
print("Stitched together pieces that belong together")

for item in final_routes:
    output2.write({'geometry': mapping(item['geometry']), 'properties': item['metadata']})
output2.close()
