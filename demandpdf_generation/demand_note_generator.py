"""
Demand Note Generator (JSON-based)
---------------------------------
Generates professional legal demand letters (DOCX)
from structured JSON metadata.

Safe image handling
Date normalization
Production-ready
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn


# =====================================================
# JSON LOADER
# =====================================================

def load_metadata_from_json(json_path: str) -> Dict[str, Any]:
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"Metadata JSON not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Normalize date fields
    for key in ["demand_creation_date", "date_of_loss"]:
        if key in data and isinstance(data[key], str):
            try:
                data[key] = datetime.fromisoformat(data[key])
            except ValueError:
                pass

    return data


# =====================================================
# DEMAND NOTE GENERATOR
# =====================================================

class DemandNoteGenerator:
    FONT_NAME = "Franklin Gothic Book"
    FONT_SIZE = 12

    def __init__(self):
        self.doc = None

    # -------------------------
    # Utility helpers
    # -------------------------

    def _set_font(self, run, size=None, bold=False, underline=False):
        run.font.name = self.FONT_NAME
        run.font.size = Pt(size or self.FONT_SIZE)
        run.font.bold = bold
        run.font.underline = underline

    def _safe_add_image(self, image_path: str, width: Inches):
        if not image_path:
            return
        path = Path(image_path)
        if not path.exists():
            print(f"⚠️ Image not found, skipping: {path}")
            return
        para = self.doc.add_paragraph()
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        para.add_run().add_picture(str(path), width=width)

    def _add_page_break(self):
        self.doc.add_page_break()

    def _add_border(self, paragraph):
        pPr = paragraph._element.get_or_add_pPr()
        pBdr = OxmlElement("w:pBdr")
        for side in ["top", "left", "bottom", "right"]:
            el = OxmlElement(f"w:{side}")
            el.set(qn("w:val"), "single")
            el.set(qn("w:sz"), "16")
            el.set(qn("w:color"), "000000")
            pBdr.append(el)
        pPr.append(pBdr)

    # -------------------------
    # Document sections
    # -------------------------

    def create_demand_note(self, metadata: Dict[str, Any], output_path: str) -> str:
        self.doc = Document()

        style = self.doc.styles["Normal"]
        style.font.name = self.FONT_NAME
        style.font.size = Pt(self.FONT_SIZE)

        self._add_logo_and_date(metadata)
        self._add_insurance_block(metadata)
        self._add_title(metadata)
        self._add_client_info(metadata)
        self._add_settlement_notice()
        self._add_salutation(metadata)
        self._add_incident(metadata)
        self._add_medical_summary(metadata)

        self._add_page_break()
        self._add_compensation(metadata)

        self.doc.save(output_path)
        return output_path

    def _add_logo_and_date(self, m):
        self._safe_add_image(m.get("logo_path"), Inches(1))

        para = self.doc.add_paragraph()
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = para.add_run(
            m.get("demand_creation_date", datetime.now()).strftime("%B %d, %Y")
            if isinstance(m.get("demand_creation_date"), datetime)
            else str(m.get("demand_creation_date"))
        )
        self._set_font(run, size=14, bold=True, underline=True)

        self.doc.add_paragraph()

    def _add_insurance_block(self, m):
        table = self.doc.add_table(rows=1, cols=2)
        table.autofit = False

        left = table.rows[0].cells[0].paragraphs[0]
        for line in [
            m.get("insurance_name"),
            f"Attn: {m.get('claim_number')}",
            m.get("insurance_address"),
            f"Tel: {m.get('insurance_telephone')}",
            f"Fax: {m.get('insurance_fax')}",
        ]:
            if line:
                run = left.add_run(line + "\n")
                self._set_font(run)

        right = table.rows[0].cells[1].paragraphs[0]
        right.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        run = right.add_run("Sent Via Certified U.S. Mail\nFacsimile")
        self._set_font(run)

        self.doc.add_paragraph()

    def _add_title(self, m):
        para = self.doc.add_paragraph()
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = para.add_run(f"*** {m.get('accident_type','ACCIDENT').upper()} POLICY LIMIT DEMAND ***")
        self._set_font(run, size=18, bold=True, underline=True)
        self.doc.add_paragraph()

    def _add_client_info(self, m):
        para = self.doc.add_paragraph()
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER

        lines = [
            f"Client: {m.get('client_name')}",
            f"Date of Loss: {m.get('date_of_loss').strftime('%m/%d/%Y')}"
            if isinstance(m.get("date_of_loss"), datetime)
            else f"Date of Loss: {m.get('date_of_loss')}",
            f"Your Insured: {m.get('defendant_name')}",
            f"Claim #: {m.get('claim_number')}",
            f"Adjuster: {m.get('defendant_adjuster')}",
        ]

        for i, line in enumerate(filter(None, lines)):
            if i:
                para.add_run("\n")
            run = para.add_run(line)
            self._set_font(run)

        self.doc.add_paragraph()

    def _add_settlement_notice(self):
        para = self.doc.add_paragraph()
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = para.add_run(
            "THIS LETTER IS FOR THE PURPOSE OF SETTLEMENT ONLY;\n"
            "IT IS NOT TO BE USED AS EVIDENCE OF ANY KIND.\n"
            "(CALIFORNIA EVIDENCE CODE SECTIONS 1152-54, ET SEQ.)\n"
            "THE CONTENTS OF THIS DEMAND SHALL NOT BE REPRODUCED\n"
            "OR REDISTRIBUTED FOR ANY PURPOSE."
        )
        self._set_font(run, bold=True)
        self.doc.add_paragraph()

    def _add_salutation(self, m):
        para = self.doc.add_paragraph()
        run = para.add_run("To Whom It May Concern:")
        self._set_font(run)

        intro = self.doc.add_paragraph()
        run = intro.add_run(
            f"This office represents {m.get('title','')}. "
            f"{m.get('client_name')} in the above-referenced matter."
        )
        self._set_font(run)
        self.doc.add_paragraph()

    def _add_incident(self, m):
        head = self.doc.add_paragraph()
        run = head.add_run("THE INCIDENT")
        self._set_font(run, size=16, bold=True, underline=True)

        self._safe_add_image(m.get("incident_image_path"), Inches(4.5))

        if m.get("incident_summary"):
            para = self.doc.add_paragraph()
            run = para.add_run(m["incident_summary"])
            self._set_font(run)

        self.doc.add_paragraph()

    def _add_medical_summary(self, m):
        if not m.get("medical_records"):
            return

        head = self.doc.add_paragraph()
        run = head.add_run("SUMMARY OF MEDICAL CARE & INJURIES")
        self._set_font(run, size=16, bold=True, underline=True)

        for rec in m["medical_records"]:
            if rec.get("summary"):
                para = self.doc.add_paragraph()
                run = para.add_run(rec["summary"])
                self._set_font(run)

            if rec.get("image_path"):
                img_para = self.doc.add_paragraph()
                img_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
                img_para.add_run().add_picture(rec["image_path"], width=Inches(4.5))
                self._add_border(img_para)

            self.doc.add_paragraph()

    def _add_compensation(self, m):
        para = self.doc.add_paragraph()
        para.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = para.add_run("CLAIM FOR COMPENSATION")
        self._set_font(run, size=16, bold=True, underline=True)

        self.doc.add_paragraph()
        run = self.doc.add_paragraph().add_run(
            f"{m.get('title','')} {m.get('client_last_name')} is entitled to full compensation under California law."
        )
        self._set_font(run)


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    metadata = load_metadata_from_json("demand_metadata.json")

    generator = DemandNoteGenerator()
    output = generator.create_demand_note(
        metadata,
        "demand_note_sample.docx"
    )

    print(f"✅ Demand note created: {output}")
