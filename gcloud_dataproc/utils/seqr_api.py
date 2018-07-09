import getpass
import os
import requests

try: input = raw_input  # python2/3
except NameError: pass

SEQR_URL = "https://seqr.broadinstitute.org"


def run(cmd):
    print(cmd)
    os.system(cmd)


def download_projects(seqr_username=None, seqr_password=None, seqr_url=SEQR_URL):
    """Returns a list of json objects representing seqr projects.


    """
    pass


def download_pedigree_info(project_guid, command_line_args, seqr_url=SEQR_URL):
    for unparsed_arg in command_line_args:
        if not unparsed_arg.startswith("gs://"):
            continue
        if ".vcf" in unparsed_arg or unparsed_arg.endswith(".vds"):
            vcf_directory = os.path.dirname(unparsed_arg)
            break
    else:
        raise ValueError("gs:// path not .vcf or .vds file not found in args")

    is_fam_file_specified = "--fam-file" in command_line_args
    is_subset_samples_file_specified = "--subset-samples" in command_line_args

    fam_filename = project_guid+".fam"
    fam_file_gcloud_path = "%(vcf_directory)s/%(fam_filename)s" % locals()
    subset_samples_filename = project_guid+"_subset_samples.txt"
    subset_samples_file_gcloud_path = "%(vcf_directory)s/%(subset_samples_filename)s" % locals()

    if (not is_fam_file_specified or not is_subset_samples_file_specified) and \
            input("(re)download fam file from seqr? [Y/n] ").lower().startswith("y"):

        seqr_username = input("seqr username: ")
        seqr_password = getpass.getpass("seqr password: ")

        # download fam file for this project
        seqr_login_url = os.path.join(seqr_url, "login")
        seqr_individual_api_url = os.path.join(seqr_url,
                                               "api/project/{0}/export_project_individuals?file_format=tsv".format(project_guid))

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

        response = s.get(seqr_individual_api_url.format(**locals()))   # get json from API
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

        # upload fam file to same place as VCF
        if not is_fam_file_specified:
            with open(fam_filename, "w") as f:
                f.write("\n".join(["\t".join(row) for row in fam_file_rows]))

            run("gsutil cp %(fam_filename)s %(fam_file_gcloud_path)s" % locals())

            command_line_args.extend(["--fam-file", fam_file_gcloud_path])

        # upload subset-samples to same place as VCF
        if not is_subset_samples_file_specified:
            with open(subset_samples_filename, "w") as f:
                f.write("\n".join([row for row in subset_samples_rows]))

            run("gsutil cp %(subset_samples_filename)s %(subset_samples_file_gcloud_path)s" % locals())

            command_line_args.extend(["--subset-samples", subset_samples_file_gcloud_path])

