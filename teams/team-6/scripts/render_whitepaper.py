"""Render the repository whitepaper draft to a portable, selectable-text PDF."""
from __future__ import annotations
import argparse
from html import escape
from pathlib import Path
import os
import re
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer


def inline(value, source_parent, output_parent):
    value=escape(value)
    def link(match):
        label,url=match.groups()
        if not re.match(r'[a-zA-Z]+:',url) and not url.startswith('#'):
            url=os.path.relpath(source_parent/url,output_parent)
        return f'<link href="{url}" color="#007F7B">{label}</link>'
    value=re.sub(r'\[([^\]]+)\]\(([^)]+)\)',link,value)
    value=re.sub(r'\*\*([^*]+)\*\*',r'<b>\1</b>',value)
    return re.sub(r'`([^`]+)`',r'<font name="Courier">\1</font>',value)


def render(source, destination):
    styles=getSampleStyleSheet()
    styles.add(ParagraphStyle('PaperTitle',fontName='Helvetica-Bold',fontSize=22,leading=27,
        textColor=colors.HexColor('#102A43'),spaceAfter=14))
    styles.add(ParagraphStyle('PaperHeading',fontName='Helvetica-Bold',fontSize=13,leading=17,
        textColor=colors.HexColor('#007F7B'),spaceBefore=15,spaceAfter=7,keepWithNext=True))
    styles.add(ParagraphStyle('PaperBody',fontName='Helvetica',fontSize=10.5,leading=14,
        textColor=colors.HexColor('#15334B'),spaceAfter=8,alignment=TA_LEFT))
    blocks=[]
    for block in source.read_text().strip().split('\n\n'):
        if block.startswith('# '):style='PaperTitle';block=block[2:]
        elif block.startswith('## '):style='PaperHeading';block=block[3:]
        else:style='PaperBody'
        blocks.append(Paragraph(inline(block.replace('\n',' '),source.parent,destination.parent),styles[style]))
    destination.parent.mkdir(parents=True,exist_ok=True)
    doc=SimpleDocTemplate(str(destination),pagesize=(8.5*inch,11*inch),leftMargin=54,rightMargin=54,
        topMargin=52,bottomMargin=52,title='Forecasting Manhattan Citi Bike departures with public data',
        author='Randy Howk',subject='DATA 622 technical whitepaper draft, October 16 2026')
    def footer(canvas,document):
        canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor('#526577'))
        canvas.drawString(54,30,'Randy Howk | DATA 622 | October 16, 2026 | Draft')
        canvas.drawRightString(8.5*inch-54,30,str(document.page))
    doc.build(blocks,onFirstPage=footer,onLaterPages=footer)
    print(destination)


if __name__=='__main__':
    root=Path(__file__).resolve().parents[1]
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,default=root/'docs/technical_whitepaper_draft.md')
    parser.add_argument('--output',type=Path,default=root/'docs/presentations/data622_whitepaper_draft.pdf')
    args=parser.parse_args();render(args.source,args.output)
