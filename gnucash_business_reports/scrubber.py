import re
from pathlib import Path


filename = "Chase9743_Activity20240626_20240725_20240821.CSV"
file_to_scrub = Path.joinpath(Path.home(), "Downloads", filename)
# file_to_scrub = Path(f"$HOME/Downloads/{filename}")
new_string = file_to_scrub.read_text()
new_string = re.sub("(?<=Amazon.com\*)(.*?)(?=\,)", "", new_string)
new_string = re.sub("(?<=AMZN Mktp US\*)(.*?)(?=\,)", "", new_string)
new_string = new_string.replace("AMZN Mktp US", "Amazon.com")
new_string = new_string.replace(
    "Amazon.com*,Shopping,", "Amazon,Liabilities:Short Term Liabilities:Amazon,"
)
new_string = new_string.replace("F&amp;F WRTHNGTN 5897", "Fuel")
new_string = new_string.replace("DisneyPLUS,Bills & Utilities,", "DisneyPLUS,Movies,")
new_string = new_string.replace(
    "SHETEK DENTAL CARE,Health & Wellness,", "Shetek Dental,Dental,"
)
new_string = new_string.replace(
    "VZWRLSS*APOCC VISN,Bills & Utilities,", "Verizon,Phone,"
)
new_string = new_string.replace(
    "Columbia Sportswear US,Shopping,", "Columbia Sportswear US,Clothing,"
)
new_string = new_string.replace("USPS PO 2687300672,Shopping,", "USPS,Shipping,")


scrubbed_file = Path.joinpath(Path.home(), "Downloads", f"scrubbed-{filename}")
scrubbed_file.write_text(new_string)
