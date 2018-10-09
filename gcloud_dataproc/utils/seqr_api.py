import os
import requests
import tempfile

SEQR_URL = "https://seqr.broadinstitute.org"


def download_pedigree_info(project_guid, seqr_url=SEQR_URL, seqr_username=None, seqr_password=None):
    """Utility method for exporting the pedigree file for the given project.

    Returns:
         2-tuple: fam_filename, subset_samples_filename
    """

    # download fam file for this project
    seqr_login_url = os.path.join(seqr_url, "login")
    seqr_individual_api_url = os.path.join(seqr_url, "api/project/{0}/export_project_individuals?file_format=tsv".format(project_guid))

    s = requests.Session()
    response = s.get(seqr_login_url)    # get CSRF cookies
    response.raise_for_status()

    csrf_token = s.cookies.get('csrftoken')
    data = {
        'username_or_email': seqr_username,
        'password': seqr_password,
        'csrfmiddlewaretoken': csrf_token,
    }
    response = s.post(seqr_login_url, data=data)    # submit login

    response.raise_for_status()

    response = s.get(seqr_individual_api_url)
    response.raise_for_status()

    content = str(response.content)

    fam_file_rows = []
    subset_samples_rows = []
    for line in content.split("\n"):
        if not line or line.startswith("#"):
            continue
        if not fam_file_rows and "affected" in line.lower() and "indiv" in line.lower() and "family" in line.lower():
            continue  # skip header row

        fields = line.split("\t")
        fam_file_rows.append(fields[0:6])
        if len(fields) > 1:
            subset_samples_rows.append(fields[1])

    fam_file_path = os.path.join(tempfile.gettempdir() or "", project_guid+".fam")
    with open(fam_file_path, "w") as f:
        f.write("\n".join(["\t".join(row) for row in fam_file_rows]))

    subset_samples_path = os.path.join(tempfile.gettempdir() or "", project_guid+"_subset_samples.txt")
    with open(subset_samples_path, "w") as f:
        f.write("\n".join([row for row in subset_samples_rows]))

    return fam_file_path, subset_samples_path
