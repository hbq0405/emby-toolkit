import unittest
import logging
from unittest.mock import Mock, patch

from handler import emby


class EmbyItemsByIdTest(unittest.TestCase):
    def test_get_emby_items_by_id_sets_limit_per_batch(self):
        if not hasattr(logging.Logger, "trace"):
            logging.Logger.trace = logging.Logger.debug

        response = Mock()
        response.json.return_value = {"Items": [{"Id": "1"}]}

        with patch.object(emby.emby_client, "get", return_value=response) as get:
            items = emby.get_emby_items_by_id(
                "http://emby",
                "api-key",
                "user-id",
                ["1"],
                fields="Id",
            )

        self.assertEqual([{"Id": "1"}], items)
        response.raise_for_status.assert_called_once()
        _, kwargs = get.call_args
        self.assertEqual(1, kwargs["params"]["Limit"])
        self.assertEqual("1", kwargs["params"]["Ids"])
        self.assertNotIn("UserId", kwargs["params"])


if __name__ == "__main__":
    unittest.main()
