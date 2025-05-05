from crawlee.crawlers import PlaywrightCrawlingContext


async def google_map_consent_check(context: PlaywrightCrawlingContext):
    if 'consent.google.com' in context.page.url:
        try:
            # Debug print current URL
            context.log.info(f"Processing consent page: {context.page.url}")
            
            # Try both accept and reject buttons
            reject_btn = await context.page.query_selector("button:has-text('Reject all'), button:has-text('Reject All')")
            
            # Debug print button status
            context.log.info(f"Consent button found: {reject_btn is not None}")
            
            if reject_btn:
                await reject_btn.click()
                context.log.info("Clicked reject button")
                await context.page.wait_for_timeout(2000)  # Wait for page to update
                context.log.info("Consent handling completed")
        except Exception as e:
            context.log.error(f"Consent handling failed: {e}")
