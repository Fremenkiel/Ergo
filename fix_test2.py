import re

with open("src/main.zig", "r") as f:
    content = f.read()

# We can use a regex to replace the expectEqualStrings block.
pattern = re.compile(r'try testing\.expectEqualStrings\(\s*"(DELETE|INSERT|UPDATE)[^;]+;', re.MULTILINE)

new_str = """try testing.expectEqualStrings(
        "DELETE\\tpublic.addresses\\t1\\t['address_line_1','city','id','country','postal_code']\\t{'address_line_1':'Googleplex','city':'Mountain View','id':'1','country':'US','postal_code':'94043'}\\t{}\\t42\\t192.168.1.50\\n" ++
        "UPDATE\\tpublic.addresses\\t1\\t['address_line_1','city','postal_code']\\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\\t42\\t192.168.1.50\\n" ++
        "INSERT\\tpublic.addresses\\t1\\t['address_line_1','city','id','country','postal_code']\\t{}\\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'1','country':'US','postal_code':'95014'}\\t42\\t192.168.1.50\\n" ++
        "DELETE\\tpublic.addresses\\t2\\t['address_line_1','city','id','country','postal_code']\\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'2','country':'US','postal_code':'95014'}\\t{}\\t42\\t192.168.1.50\\n" ++
        "UPDATE\\tpublic.addresses\\t2\\t['address_line_1','city','postal_code']\\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\\t42\\t192.168.1.50\\n" ++
        "INSERT\\tpublic.addresses\\t2\\t['address_line_1','city','id','country','postal_code']\\t{}\\t{'address_line_1':'Googleplex','city':'Mountain View','id':'2','country':'US','postal_code':'94043'}\\t42\\t192.168.1.50\\n",
        ch_result.stdout,
    );"""

match = pattern.search(content)
if match:
    new_content = content[:match.start()] + new_str + content[match.end():]
    with open("src/main.zig", "w") as f:
        f.write(new_content)
    print("Replaced successfully.")
else:
    print("Could not find the block to replace.")

