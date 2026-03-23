from app.services.crawl import CrawledPage
from enrichment.contact_extractor import extract_contacts


def test_extract_contacts_from_multiple_page_types():
    pages = [
        CrawledPage("homepage", "https://example.com", "", "", "Call (555) 123-4567", "requests", "ok"),
        CrawledPage(
            "team",
            "https://example.com/team",
            "",
            "",
            "Owner Jane Smith jane@acme.com direct line 555-222-3333",
            "requests",
            "ok",
        ),
    ]

    result = extract_contacts(pages)

    assert any(item.type == "name" and item.value == "Jane Smith" for item in result.items)
    assert any(item.type == "email" and item.value == "jane@acme.com" for item in result.items)
    assert any(item.type == "phone" and "555-222-3333" in item.value for item in result.items)
