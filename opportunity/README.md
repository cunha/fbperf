# Notes

This graph plots the performance of the *primary* (preferred) route vs
the *best alternate* (less preferred) route.  We do *not* compare
*primary* vs *best* because this is identical to comparing vs *best
alternate* except that you can never get any improvement.

When there are no alternate routes, we either: ignore the row (`-w-alt`
files) or use the primary itself as an alterante (`-all` files).  In the
case where the primary path was missing (because it did not have enough
samples), we just ignore the row; this case should be unusual as the
primary path has higher sampling rates than other routes.

Also, if an alternate route is missing (e.g., the second most
preferred), we *will* continue processing the other alternate routes and
include compare the primary against any alternate route with enough
samples.
