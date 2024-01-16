from bs4 import BeautifulSoup
from collections import namedtuple
import subprocess

Project = namedtuple("Project", ["name", "sample_type", "reference_genome"])


def to_v03_dataset_type_from_v02(dataset_type: str, sample_type: str) -> str:
    if dataset_type == "SNV" or dataset_type == "VARIANTS":
        return "SNV_INDEL"
    if dataset_type == "SV" and sample_type == "WES":
        return "GCNV"
    if dataset_type == "SV" and sample_type == "WGS":
        return "SV"
    if dataset_type == "MITO":
        return "MITO"
    raise RuntimeError


def parse_v2_projects():
    projects = set()
    with open("table.html", "r") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    tr_elements = soup.find("tbody").find_all("tr")
    for tr in tr_elements:
        index_name = tr.find("td", class_="").get_text(strip=True)
        try:
            project_name = (
                tr.find("td", class_="")
                .find_next("td")
                .find("a")["href"]
                .replace("/project/", "")
                .replace("/project_page", "")
            )
        except:
            # yesterday loaded SV indexes not attached to projects
            project_name = None
        dataset_type = (
            tr.find("td", class_="")
            .find_next("td")
            .find_next("td")
            .get_text(strip=True)
        )
        sample_type = (
            tr.find("td", class_="")
            .find_next("td")
            .find_next("td")
            .find_next("td")
            .get_text(strip=True)
        )
        genome_version = (
            tr.find("td", class_="")
            .find_next("td")
            .find_next("td")
            .find_next("td")
            .find_next("td")
            .get_text(strip=True)
        )

        if project_name:
            projects.add(
                Project(
                    project_name,
                    to_v03_dataset_type_from_v02(dataset_type, sample_type),
                    "GRCh38" if genome_version == "38" else "GRCh37",
                )
            )
    return projects


def parse_v3_projects():
    projects = set()
    for reference_genome in ["GRCh37", "GRCh38"]:
        for dataset_type in ["SNV_INDEL", "MITO", "SV", "GCNV"]:
            path = f"gs://seqr-hail-search-data/v03/{reference_genome}/{dataset_type}/projects/"
            r = subprocess.run(
                ["gsutil", "ls", path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            if "matched no objects" in r.stderr:
                continue
            for line in r.stdout.strip().split("\n"):
                if line.endswith('projects/'):
                    continue
                projects.add(
                    Project(
                        line.split("/")[7].replace(".ht", ""),
                        line.split("/")[5],
                        line.split("/")[4],
                    )
                )
    return projects


def main():
    v2_projects = parse_v2_projects()
    v3_projects = parse_v3_projects()

    print("V2 projects")
    print(
        "37 SNV_INDEL projects",
        len(
            list(
                p
                for p in v2_projects
                if p.sample_type == "SNV_INDEL" and p.reference_genome == "GRCh37"
            )
        ),
    )
    print(
        "38 SNV_INDEL projects",
        len(
            list(
                p
                for p in v2_projects
                if p.sample_type == "SNV_INDEL" and p.reference_genome == "GRCh38"
            )
        ),
    )
    print(
        "38 MITO projects",
        len(
            list(
                p
                for p in v2_projects
                if p.sample_type == "MITO" and p.reference_genome == "GRCh38"
            )
        ),
    )
    print(
        "38 SV projects",
        len(
            list(
                p
                for p in v2_projects
                if p.sample_type == "SV" and p.reference_genome == "GRCh38"
            )
        ),
    )
    print(
        "38 GCNV projects",
        len(
            list(
                p
                for p in v2_projects
                if p.sample_type == "GCNV" and p.reference_genome == "GRCh38"
            )
        ),
    )

    print("V3 projects")
    print(
        "37 SNV_INDEL projects",
        len(
            list(
                p
                for p in v3_projects
                if p.sample_type == "SNV_INDEL" and p.reference_genome == "GRCh37"
            )
        ),
    )
    print(
        "38 SNV_INDEL projects",
        len(
            list(
                p
                for p in v3_projects
                if p.sample_type == "SNV_INDEL" and p.reference_genome == "GRCh38"
            )
        ),
    )
    print(
        "38 MITO projects",
        len(
            list(
                p
                for p in v3_projects
                if p.sample_type == "MITO" and p.reference_genome == "GRCh38"
            )
        ),
    )
    print(
        "38 SV projects",
        len(
            list(
                p
                for p in v3_projects
                if p.sample_type == "SV" and p.reference_genome == "GRCh38"
            )
        ),
    )
    print(
        "38 GCNV projects",
        len(
            list(
                p
                for p in v3_projects
                if p.sample_type == "GCNV" and p.reference_genome == "GRCh38"
            )
        ),
    )

    print("V2 not in V3")
    for project in (v2_projects - v3_projects):
        print(project)

    print("V3 not in V2")
    for project in (v3_projects - v2_projects):
        print(project)


if __name__ == "__main__":
    main()
