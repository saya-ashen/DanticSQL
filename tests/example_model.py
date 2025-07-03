import datetime
from enum import Enum
from typing import Any, cast

from pydantic import computed_field
from sqlmodel import Field, Relationship

from danticsql import SQLModel

def _(text):
    return text

class CpgEnum(str, Enum):
    shelf = "Shelf"
    shore = "Shore"
    island = "Island"
    other = "Other"


class Gene(SQLModel, table=True):
    """
    Represents the gene information.
    """

    __tablename__ = "gene_view"  # type: ignore
    __table_args__ = ({"info": {"title": _("基因")}},)

    gene_id: int = Field(..., primary_key=True, description="Gene 唯一编号,用作外键")
    gene_name: str | None = Field(
        None,
        alias=_("基因名称"),
        description="Gene 名称，例如：TP53",
    )
    # FIXME: 修改视图中的定义
    # gene_location: str | None = Field(None, alias=_("基因位置"), description="Gene位置，格式：chr1:123456-123789")
    ensembl_id: str | None = Field(None, alias=_("Ensembl_id"), description="Ensembl ID, 例如：ENSG00000141510")
    # FIXME: 修改视图中的定义
    # promoter_body: str | None = Field(
    #     None, alias=_("promoter body 占比"), description="promoter body 各自的占比,格式：promoter:20% / body:80%"
    # )
    probes: list["Probe"] = Relationship(back_populates="gene")

    # NOTE: 数据库中不存在该关联，这里添加是方便前端显示
    traits: list["Trait"] = Relationship()

    @computed_field(alias=_("雷达图"))
    @property
    def radar(self) -> dict[str, list[str] | str]:
        return {
            "content": f"![{self.gene_name}](api/ewas/gene/{self.gene_name}/radar/image)",
            "classes": ["ewas-gene-image"],
        }

    @computed_field(alias=_("关联位点"))
    @property
    def gene_probes(self) -> list[str]:
        probes = set([probe.probe_id for probe in self.probes])
        return list(probes)

    @computed_field(alias=_("关联性状"))
    @property
    def gene_traits(self) -> list[str]:
        # HACK: This is a hack
        traits = set([trait.trait_name for trait in self.traits])
        return list(traits)


class StudyProbeAssociation(SQLModel, table=True):
    """
    Represents the association between study and probe.
    """

    __tablename__ = "association_view"  # type: ignore
    # FIXME:
    # __tablename__ = "study_probe_association_view"  # type: ignore

    study_id: int = Field(
        ..., primary_key=True, foreign_key="study_view.study_id", description="研究对应的唯一编号，用做外键"
    )
    probe_id: str = Field(
        ..., primary_key=True, foreign_key="probe_view.probe_id", description="probe 唯一编号,用作外键"
    )


class Probe(SQLModel, table=True):
    """
    Represents the probe information.
    """

    __tablename__ = "probe_view"  # type: ignore
    __table_args__ = ({"info": {"title": _("位点")}},)

    probe_id: str = Field(
        ...,
        primary_key=True,
        alias=_("Probe ID"),
        description="probe 唯一编号,用作外键",
    )
    # FIXME: 修改视图中的定义
    # cpg_classification: CpgEnum | None = Field(
    #     None,
    #     alias=_("CpG classification"),
    #     description="该 probe 的 CpG_classification，例如：Island，Shore，Shelf，Other ",
    # )
    # chr: int | None = Field(None, description="probe 所在的染色体")
    # FIXME: 修改视图中的定义
    # probe_location: str | None = Field(None, alias=_("位置"), description="probe 位置，格式：chr1:123456-123789")
    gene_id: int | None = Field(None, foreign_key="gene_view.gene_id", description="probe 对应的基因id")
    gene: Gene = Relationship(back_populates="probes")
    studies: list["Study"] = Relationship(back_populates="probes", link_model=StudyProbeAssociation)
    traits: list["Trait"] = Relationship()

    @computed_field(alias=_("雷达图"))
    @property
    def radar(self) -> dict[str, list[str] | str]:
        return {
            "content": f"![{self.probe_id}](api/ewas/probe/{self.probe_id}/radar/image)",
            "classes": ["ewas-probe-image"],
        }

    @computed_field(alias=_("关联研究"))
    @property
    def probe_studies(self) -> list[str]:
        # HACK:
        return list(set([study.publication_title for study in self.studies if study.publication_title]))

    @computed_field(alias=_("关联基因"))
    def probe_gene(self) -> str | None:
        return self.gene.gene_name if self.gene else None

    @computed_field(alias=_("关联性状"))
    @property
    def probe_traits(self) -> list[str]:
        # HACK:
        traits = set([trait.trait_name for trait in self.traits])
        return list(traits)


class Study(SQLModel, table=True):
    __tablename__ = "study_view"  # type: ignore
    __table_args__ = ({"info": {"title": _("研究")}},)

    study_id: int = Field(
        ...,
        primary_key=True,
        description="研究对应的唯一编号，用做外键",
    )
    case_group: str | None = Field(None, description="")  # Assuming string based on lack of specific type
    control_group: str | None = Field(None, description="")  # Assuming string based on lack of specific type
    publication_title: str | None = Field(None, alias=_("文章标题"), description="研究对所在文献的标题")
    publication_pmid: int | None = Field(None, alias=_("文章PMID"), description="研究所在文献的pmid")
    publication_date: datetime.date | None = Field(
        None, alias=_("发布日期"), description="文献发布日期"
    )  # Could be date/datetime depending on format
    ontology_details: str | None = Field(None, alias=_("GO标签"), description="GO 标签")
    probes: list["Probe"] = Relationship(back_populates="studies", link_model=StudyProbeAssociation)
    traits: list["Trait"] = Relationship(back_populates="study")

    @computed_field(alias=_("关联位点"))
    @property
    def study_probes(self) -> str:
        return "\n".join([probe.probe_id for probe in self.probes])

    @computed_field(alias=_("关联性状"))
    @property
    def study_traits(self) -> str:
        return "\n".join([trait.trait_name for trait in self.traits])


class Trait(SQLModel, table=True):
    """
    Represents the trait information.
    """

    __tablename__ = "trait_view"  # type: ignore
    __table_args__ = ({"info": {"title": _("性状")}},)

    trait_name: str = Field(
        ...,
        primary_key=True,
        alias=_("Trait名称"),
        description="性状名称,例如吸烟，BMI, 血压等",
    )
    type_name: str | None = Field(
        None,
        alias=_("Trait类型"),
        description="性状类型名，包括：phenotype, behavior, environmental factor, non-cancer disease, cancer",
    )
    study_id: int | None = Field(None, foreign_key="study_view.study_id", description="特征对应的研究id,外键")
    study: Study = Relationship(back_populates="traits")
    gene: Gene = Relationship(back_populates="traits", sa_relationship_kwargs={"uselist": False})

    # NOTE: 数据库中不存在的关联
    gene_id: int | None = Field(None, foreign_key="gene_view.gene_id")
    probe_id: str | None = Field(None, foreign_key="probe_view.probe_id")

    @computed_field(alias=_("研究来源"))
    @property
    def trait_study(self) -> str | None:
        if self.study:
            return self.study.publication_title
        else:
            return None


def summarize_item(item: SQLModel) -> dict[str, Any]:
    """Helper function to summarize a single SQLModel item."""
    summary = {}
    if isinstance(item, Gene):
        summary = {
            "gene_name": item.gene_name,
            "ensembl_id": item.ensembl_id,
        }
        associated_probes = item.probes
        # 把probes 按照 studies的数量排序
        associated_probes = sorted(associated_probes, key=lambda x: len(x.studies), reverse=True)
        associated_probes = [probe.probe_id for probe in associated_probes]
        if len(associated_probes) > 10:
            associated_probes_summary = (
                f"共{len(associated_probes)}个关联位点，其中报道次数排名前10的为：{', '.join(associated_probes[:10])}"
            )
            summary["associated_probes_summary"] = associated_probes_summary
        elif len(associated_probes) > 0:
            associated_probes_summary = f"关联位点为：{', '.join(associated_probes)}"
            summary["associated_probes_summary"] = associated_probes_summary

    elif isinstance(item, Probe):
        summary = {
            "probe_id": item.probe_id,
            "related_gene": item.gene.gene_name if item.gene else "",
        }
        associated_traits = item.probe_traits
        if len(associated_traits) > 10:
            associated_traits_summary = (
                f"共{len(associated_traits)}个关联特征，其中前10个为：{', '.join(associated_traits[:10])}"
            )
            summary["associated_traits_summary"] = associated_traits_summary
        elif len(associated_traits) > 0:
            associated_traits_summary = f"关联特征为：{', '.join(associated_traits)}"
    elif isinstance(item, Study):
        summary = {
            "publication_pmid": item.publication_pmid,
            "publication_title": item.publication_title,
            "publication_date": str(item.publication_date) if item.publication_date else None,
        }
        associated_probes = item.study_probes
        associated_traits = item.study_traits
        if len(associated_probes) > 10:
            associated_probes_summary = (
                f"共{len(associated_probes)}个关联位点，其中前10个为：{', '.join(associated_probes[:10])}"
            )
        elif len(associated_probes) > 0:
            associated_probes_summary = f"关联位点为：{', '.join(associated_probes)}"

        if len(associated_traits) > 10:
            associated_traits_summary = (
                f"共{len(associated_traits)}个关联特征，其中前10个为：{', '.join(associated_traits[:10])}"
            )
        elif len(associated_traits) > 0:
            associated_traits_summary = f"关联特征为：{', '.join(associated_traits)}"

    elif isinstance(item, Trait):
        summary = {
            "trait_name": item.trait_name,
            "trait_type": item.type_name,
            "source_study_id_summary": item.trait_study,  # Uses the computed field
        }
    # 去除 None 的字段
    summary = {k: v for k, v in summary.items() if v is not None}
    return summary


def summarize_data(data: dict[str, list[SQLModel]], max_examples_per_type: int = 10):
    """
    Summarizes the retrieved SQLModel data into a more compact format for LLMs.

    Args:
        data: A dictionary where keys are entity type names (e.g., "genes")
              and values are lists of SQLModel instances.
        max_examples_per_type: The maximum number of example items to detail for each entity type.

    Returns:
        A dictionary containing the summary.
    """
    overall_summary = {}

    for entity_type, items_list in data.items():
        entity_summary = {}
        count = len(items_list)
        entity_summary["count"] = count

        if count > 0:
            title = items_list[0].__class__.__table__.info.get("title")  # type: ignore
            if not title:
                continue  # Skip if no title is found

            # 把probes 按照 studies的数量排序
            if entity_type == "probe_view":
                items_list = sorted(items_list, key=lambda x: len(cast(Probe, x).studies), reverse=True)

            example_summaries = []
            for i, item in enumerate(items_list):
                example_summaries.append(summarize_item(item))
                if i >= max_examples_per_type:
                    break
            entity_summary["examples"] = example_summaries
            overall_summary[title] = entity_summary
        else:
            continue  # Skip empty entity types

    high_level_text = "总结: "
    parts = [f"{overall_summary[k]['count']} {k}" for k in overall_summary if overall_summary[k]["count"] > 0]
    if not parts:
        high_level_text += "no entities."
    else:
        high_level_text += ", ".join(parts) + "."
    overall_summary["text_description"] = high_level_text
    summary_text = high_level_text
    summary_text += str(overall_summary)

    return summary_text

