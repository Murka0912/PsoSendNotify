import xml.etree.ElementTree  as ET

def description_parser(xmltext):
    root_node = ET.parse(xmltext).getroot()
    print(root_node)